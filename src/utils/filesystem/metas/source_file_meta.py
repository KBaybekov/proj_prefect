# -*- coding: utf-8 -*-
"""
Хранение метаданных исходных файлов
"""
from utils.logger import get_logger
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import blake2s as hashlib_blake2s
from pathlib import Path
from typing import Any, Dict, Optional

logger = get_logger(name=__name__)

# Класс для хранения метаданных исходных файлов
@dataclass(slots=True)
class SourceFileMeta:
    """
    Класс для хранения метаданных исходных файлов.
    """
    filepath: Path
    symlink_dir: Path
    name: str  = field(default_factory=str)
    directory: Path = field(default_factory=Path)
    symlink: Path = field(default_factory=Path)
    extension: str = field(default_factory=str)
    basename:str = field(default_factory=str)
    quality_pass: bool = field(default_factory=bool)
    batch: str = field(default_factory=str)
    sample: str = field(default_factory=str)
    size: int = field(default=0)
    created: Optional[datetime] = field(default=None)
    modified: Optional[datetime] = field(default=None)
    dev: int = field(default=0)
    ino:  int = field(default=0)
    nlink: int = field(default=0)
    status: str = field(default_factory=str)
    # Список изменений в файле по сравнению с предыдущей версией (если есть)
    changes: Dict[str, int | datetime] = field(default_factory=dict)
    # Отпечаток предыдущей версии файла (если есть)
    previous_version: str = field(default_factory=str)
    fingerprint: str = field(default_factory=str)

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any]
                 ) -> 'SourceFileMeta':
        """
        Создаёт объект BatchMeta из документа БД.

        :param doc: Документ из коллекции 'batches' в MongoDB.
        :return: Объект BatchMeta.
        """
        # Инициализируем основные поля SourceFileMeta
        return SourceFileMeta(
                              filepath=Path(doc.get("filepath", "")),
                              symlink_dir=Path(doc.get("symlink_dir", "")),
                              name=doc.get("name", ""),
                              directory=Path(doc.get("directory", "")),
                              symlink=Path(doc.get("symlink", "")),
                              extension=doc.get("extension", ""),
                              basename=doc.get("basename", ""),
                              quality_pass=doc.get("quality_pass", False),
                              batch=doc.get("batch", ""),
                              sample=doc.get("sample", ""),
                              size=doc.get("size", 0),
                              created=doc.get("created"),
                              modified=doc.get("modified"),
                              dev=doc.get("dev", 0),
                              ino=doc.get("ino", 0),
                              nlink=doc.get("nlink", 0),
                              status=doc.get("status", "indexed"),
                              changes=doc.get("changes", {}),
                              previous_version=doc.get("previous_version", ""),
                              fingerprint=doc.get("fingerprint", "")
                             )

    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Конвертирует объект SourceFileMeta в словарь.
        """
        dict_obj = self.__dict__
        for key in [
                    "filepath",
                    "symlink_dir",
                    "symlink",
                    "directory"
                   ]:
            if key in dict_obj:
                dict_obj[key] = dict_obj[key].as_posix()
            else:
                dict_obj[key] = ""
        return dict_obj

    def __post_init__(
                      self
                     ) -> None :
        # Получаем данные по принадлежности файла
        self.batch = '_'.join(str(self.filepath.parts[-3]).split('_')[3:])
        self.sample = self.filepath.parts[-4]
        self.symlink =  self.symlink_dir / Path(":".join([self.batch,
                                                          self.sample,
                                                          self.filepath.name]))
        # Получаем метаданные файла 
        stat = self.filepath.stat()
        self.size = stat.st_size
        self.created = datetime.fromtimestamp(stat.st_ctime).astimezone(timezone.utc)
        self.modified = datetime.fromtimestamp(stat.st_mtime).astimezone(timezone.utc)
        self.dev = stat.st_dev
        self.ino = stat.st_ino
        self.nlink = stat.st_nlink
        # Формируем отпечаток файла для отслеживания изменений
        h = hashlib_blake2s()
        for v in (
                  self.size,
                  self.dev,
                  self.ino,
                  self.modified.timestamp()
                 ):
            h.update(str(v).encode())
            h.update(b'|')
        self.fingerprint = h.hexdigest()
        return None
        
    def finalize(
                 self
                ) -> None :
        def _is_qc_pass(file: Path) -> bool:
            """
            Принимает полный путь к файлу и возвращает True, если:
            - в имени файла есть "pass";
            - в имени папки файла есть "pass";
            - ни в имени файла, ни в имени папки файла нет "pass"/"fail"/"skip".
            Возвращает False в случае наличия "fail"/"skip"
            """
            for name in [file.name, file.parent.name]:
                if "_pass" in name:
                    return True
                elif any([fail_flag in name for fail_flag in ['_fail', '_skip']]):
                    return False
            return True
        
        # Получаем данные по частям пути файла
        # Имя файла без директорий и расширения
        self.basename = self.filepath.name.split('.')[0]
        self.directory = self.filepath.parent
        raw_extensions = self.filepath.suffixes
        # Расширение файла без точек и .gz 
        self.extension = next(f.lower().removeprefix('.')
                               for f in raw_extensions
                               if f in ['.fast5', '.fastq', '.fq', '.pod5'])
        self.quality_pass = _is_qc_pass(self.filepath)
        self.name = self.symlink.name
        self.status = 'indexed'
        return None
