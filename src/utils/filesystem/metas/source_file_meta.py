# -*- coding: utf-8 -*-
"""
Модуль предоставляет класс SourceFileMeta для сбора, хранения и управления
метаданными отдельных файлов, поступающих в систему.
Содержит информацию о расположении, состоянии, свойствах и принадлежности файла к батчу/образцу.
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
    Класс для хранения метаданных исходного файла.

    Автоматически извлекает информацию из пути к файлу и его статуса в ФС.
    Генерирует уникальный отпечаток (fingerprint) на основе ключевых свойств,
    позволяющий отслеживать изменения содержимого и перемещения.
    """
    filepath: Path
    """
    Абсолютный путь к исходному файлу в файловой системе.
    """

    symlink_dir: Path
    """
    Директория, в которой будет создан симлинк на файл.
    Определяется на основе расширения файла.
    """

    name: str  = field(default_factory=str)
    """
    Имя симлинка (обычно совпадает с именем файла).
    Устанавливается при вызове finalize().
    """

    directory: Path = field(default_factory=Path)
    """
    Директория, в которой находится исходный файл.
    Заполняется из filepath при инициализации.
    """

    symlink: Path = field(default_factory=Path)
    """
    Путь к симлинку, создаваемому для файла.
    Формат: {symlink_dir}/{batch}:{sample}:{filename}.
    """

    extension: str = field(default_factory=str)
    """
    Расширение файла без точек (например, 'fastq', 'fast5').
    Определяется из суффиксов пути.
    """

    basename:str = field(default_factory=str)
    """
    Имя файла без расширения (например, 'sample_001_R1').
    """

    batch: str = field(default_factory=str)
    """
    Имя батча, к которому принадлежит файл.
    Извлекается из структуры пути (обычно третья часть с конца).
    """

    sample: str = field(default_factory=str)
    """
    Имя образца, к которому принадлежит файл.
    Извлекается из структуры пути (обычно четвёртая часть с конца).
    """

    size: int = field(default=0)
    """
    Размер файла в байтах.
    """

    created: Optional[datetime] = field(default=None)
    """
    Дата и время создания файла (время изменения inode).
    Хранится в UTC.
    """

    modified: Optional[datetime] = field(default=None)
    """
    Дата и время последнего изменения содержимого файла.
    Хранится в UTC.
    """

    dev: int = field(default=0)
    """
    Идентификатор устройства (dev), на котором находится файл.
    Часть уникального ключа файла в ФС.
    """

    ino:  int = field(default=0)
    """
    Номер inode файла.
    Часть уникального ключа файла в ФС.
    """

    nlink: int = field(default=0)
    """
    Количество жёстких ссылок на файл.
    """

    status: str = field(default_factory=str)
    """
    Текущий статус файла. Возможные значения:
    - 'indexed' — проиндексирован
    - 'deprecated' — устарел
    - 'deleted' — удалён
    """

    changes: Dict[str, int | datetime] = field(default_factory=dict)
    """
    Словарь изменений по сравнению с предыдущей версией файла.
    Ключ — название свойства, значение — новое значение.
    """

    previous_version: str = field(default_factory=str)
    """
    Отпечаток предыдущей версии файла.
    Используется для установки связей между версиями и пометки старой версии как устаревшей.
    """

    fingerprint: str = field(default_factory=str)
    """
    Криптографический отпечаток файла, вычисляемый на основе:
    - размера
    - dev
    - ino
    - времени модификации
    Используется для проверки целостности и отслеживания изменений.
    """

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any]
                 ) -> 'SourceFileMeta':
        """
        Создаёт экземпляр SourceFileMeta из документа MongoDB.

        :param doc: Словарь с данными из коллекции 'files'.
        :type doc: Dict[str, Any]
        :return: Инициализированный объект SourceFileMeta.
        :rtype: SourceFileMeta
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
        Преобразует объект SourceFileMeta в словарь для сохранения в MongoDB.

        Выполняет сериализацию полей Path в строки с помощью .as_posix().

        :return: Сериализованный словарь с метаданными файла.
        :rtype: Dict[str, Any]
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
        """
        Выполняется после инициализации экземпляра.
        Извлекает метаданные из пути к файлу и статуса в ФС.
        """
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
        """
        Финализирует метаданные файла: определяет расширение, базовое имя, качество и устанавливает статус.

        Должен вызываться перед сохранением в БД.
        """
        # Получаем данные по частям пути файла
        # Имя файла без директорий и расширения
        self.basename = self.filepath.name.split('.')[0]
        self.directory = self.filepath.parent
        raw_extensions = self.filepath.suffixes
        # Расширение файла без точек и .gz 
        self.extension = next(f.lower().removeprefix('.')
                               for f in raw_extensions
                               if f in ['.fast5', '.fastq', '.fq', '.pod5'])
        self.name = self.symlink.name
        self.status = 'indexed'
        return None
