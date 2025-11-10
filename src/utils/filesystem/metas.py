# -*- coding: utf-8 -*-
"""
Файловая система: индексация исходников, создание симлинков и базовая мета
"""
from __future__ import annotations
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime, timezone
from utils.refs import REFS
from utils.logger import get_logger

logger = get_logger(name=__name__)

pore_data: Dict[str, Dict[str, Any]]
refs_version, pore_data  = REFS.get()


@dataclass(slots=True)
class FileGroup:
    """Подгруппа файлов (pass/fail): список симлинков и суммарный размер."""
    files: Set[Path] = field(default_factory=set)
    size: int = 0

    def add2file_group(self, file: Path, size: int) -> bool:
        """
        Добавляет файл в группу файлов, если он ещё не был добавлен.
        Возвращает True, если файл был добавлен
        """
        file_added: bool = False
        if file not in self.files:
            self.files.add(file)
            old_size = self.size
            self.size += size
            logger.debug(f"Добавлен файл: {file}. Размер FileGroup изменён: {old_size} -> {self.size}")
            file_added = True
        else:
            logger.debug(f"Файл {file} уже был добавлен в FileGroup")
        return file_added

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        """
        Проходит по словарю изменений и обновляет размеры файлов в группе.
        Возвращает True, если размеры были изменены
        """
        size_changed: bool = False
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            size_diffs = mod_data.get('size')
            if size_diffs:
                diff = size_diffs[1] - size_diffs[0]
                if diff:
                    old_size = self.size
                    self.size += diff
                    logger.debug(f"Размер FileGroup изменён: {old_size} -> {self.size}")
                    size_changed = True
        return size_changed
        
    def remove_file_from_group(self, file:Path, size:int) -> bool:
        """
        Удаляет файл из группы файлов, если он был добавлен.
        Возвращает True, если файл был удалён
        """
        file_removed: bool = False
        if file in self.files:
            self.files.remove(file)
            old_size = self.size
            self.size -= size
            logger.debug(f"Удалён файл: {file}. Размер FileGroup изменён: {old_size} -> {self.size}")
            file_removed = True
        return file_removed


@dataclass(slots=True)
class FileSubset:
    """
    Набор файлов одного типа (напр., fast5 или pod5):
        • size  — общий размер (pass + fail)
        • pass_ — группа с файлами, прошедшими QC
        • fail  — группа с файлами, не прошедшими QC
    """
    size: int = 0
    pass_: FileGroup = field(default_factory=FileGroup)
    fail:  FileGroup = field(default_factory=FileGroup)

    @staticmethod
    def from_db(doc: Dict[str, Any]) -> FileSubset:
        """
        Восстанавливает объект FileSubset из документа БД.
        """
        return FileSubset(
                          size=doc.get("size", 0),
                          pass_=FileGroup(
                                          files={Path(f) for f in doc.get("pass", {}).get("files", set())},
                                          size=doc.get("pass", {}).get("size", 0)
                                         ),
                          fail=FileGroup(
                                         files={Path(f) for f in doc.get("fail", {}).get("files", set())},
                                         size=doc.get("fail", {}).get("size", 0)
                                        ),
                         )


    def add2file_subset(self, file: Path, size: int, qc_pass: bool) -> bool:
        """
        Добавляет файл в сабсет файлов (в зависимости от флага QC), если он ещё не был добавлен.
        Возвращает True, если файл был добавлен
        """
        file_added: bool = False
        if qc_pass:
            logger.debug(f"Добавляем файл {file} в FileSubset 'pass_'")
            file_added = self.pass_.add2file_group(file, size)
        else:
            logger.debug(f"Добавляем файл {file} в FileSubset 'fail'")
            file_added = self.fail.add2file_group(file, size)
        if file_added:
            self.size += size
        return file_added

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        subset_changed: bool = False
        if not edit_dict:
            logger.debug("Изменений в метаданных нет")
            return subset_changed
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            size_diffs = mod_data.get('size')
            if size_diffs:
                diff = size_diffs[1] - size_diffs[0]
                if diff:
                    old_size = self.size
                    self.size += diff
                    logger.debug(f"Размер FileSubset изменён: {old_size} -> {self.size}")
                    subset_changed = True
        return subset_changed

    def remove_file_from_subset(self, file:Path, size:int, qc_pass:bool) -> bool:
        """
        Удаляет файл из сабсета файлов, если он был добавлен.
        Возвращает True, если файл был удалён
        """
        file_removed: bool = False
        if qc_pass:
            logger.debug(f"Удаляем файл {file} из FileSubset 'pass_'")
            file_removed = self.pass_.remove_file_from_group(file, size)
        else:
            logger.debug(f"Удаляем файл {file} из FileSubset 'fail'")
            file_removed = self.fail.remove_file_from_group(file, size)
        if file_removed:
            self.size -= size
        return file_removed


@dataclass(slots=True)
class FileSet:
    """
    Набор файлов, относящийся к одному объекту (батчу/образцу), включает:
        • fast5 - набор файлов fast5
        • pod5 - набор файлов pod5
        • fq - набор файлов fq
        • size - общий размер файлов
    """
    size_total: int = 0
    size_pass: int = 0
    size_fail: int = 0
    # Счётчик для размера нормальных fast5/pod5
    size_pass_sourcefiles: int = 0
    # Сохраняем файл и его отпечаток
    files: Dict[Path, str] = field(default_factory=dict)
    fast5: FileSubset = field(default_factory=FileSubset)
    pod5: FileSubset = field(default_factory=FileSubset)
    fq: FileSubset = field(default_factory=FileSubset)

    @staticmethod
    def from_db(doc: Dict[str, Any]) -> FileSet:
        """
        Восстанавливает объект FileSet из документа БД.
        """
        return FileSet(
                        size_total=doc.get("size_total", 0),
                        size_pass=doc.get("size_pass", 0),
                        size_fail=doc.get("size_fail", 0),
                        size_pass_sourcefiles=doc.get("size_pass_sourcefiles", 0),
                        files={Path(k): v for k, v in doc.get("files", {}).items()},
                        fast5=FileSubset.from_db(doc.get("fast5", {})),
                        pod5=FileSubset.from_db(doc.get("pod5", {})),
                        fq=FileSubset.from_db(doc.get("fq", {}))
                        )

    def _file_added_to_fileset(
                               self,
                               file: Path,
                               fingerprint:str,
                               extension: str,
                               size: int,
                               qc_pass: bool
                              ) -> bool:
        if file in self.files:
            return False
        else:
            file_added = self.add2file_set(
                                           file,
                                           fingerprint,
                                           extension,
                                           size,
                                           qc_pass
                                          )
            return file_added

    def add2file_set(self, file: Path, fingerprint:str, extension: str, size: int, qc_pass: bool) -> bool:
        """
        Добавляет файл в сет файлов (в зависимости от расширения) 
        и соответствующий FileSubset, если он ещё не был добавлен.
        Возвращает True, если файл был добавлен
        """
        file_added: bool = False

        if file not in self.files:
            # выбираем сабсет на основе расширения файла
            if "fast5" in extension:
                logger.debug(f"Добавляем файл {file} в FileSubset 'fast5'")
                file_added = self.fast5.add2file_subset(file, size, qc_pass)
            elif "pod5" in extension:
                logger.debug(f"Добавляем файл {file} в FileSubset 'pod5'")
                file_added = self.pod5.add2file_subset(file, size, qc_pass)
            elif any([
                    fq_ext in extension
                    for fq_ext in ['fq', 'fastq']
                    ]):
                logger.debug(f"Добавляем файл {file} в FileSubset 'fq'")
                file_added = self.fq.add2file_subset(file, size, qc_pass)
            else:
                logger.debug(f"Неизвестный тип файла: {extension}; файл не добавлен в FileSubset")
                return False

            if file_added:
                self.files[file] = fingerprint
                # Добавляем размер файла к общему размеру
                self.size_total += size
                if qc_pass:
                    self.size_pass += size
                    if extension in ['fast5', 'pod5']:
                        self.size_pass_sourcefiles += size
                else:
                    self.size_fail += size
        return file_added

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        """
        Функция для обновления метаданных файлов в FileSet
        Возвращает True, если были изменены метаданные
        """
        fileset_changed: bool = False
        # проходим по словарям изменений
        for file, mod_data in edit_dict.items():
            size_diffs = mod_data.get('size')
            if size_diffs:
                diff = size_diffs[1] - size_diffs[0]
                if diff:
                    old_total = self.size_total
                    self.size_total += diff
                    logger.debug(f"Общий размер FileSet изменён: {old_total} -> {self.size_total}")
                    fileset_changed = True
                    if 'pass' in file.name:
                        old_pass = self.size_pass
                        self.size_pass += diff
                        logger.debug(f"Общий размер файлов 'pass' изменён: {old_pass} -> {self.size_pass}")
                        if file.suffix in ['.fast5', '.pod5']:
                            old_sourcefiles = self.size_pass_sourcefiles
                            self.size_pass_sourcefiles += diff
                            logger.debug(f"Общий размер исходных файлов изменён: {old_sourcefiles} -> {self.size_pass_sourcefiles}")
                    else:
                        old_fail = self.size_fail
                        self.size_fail += diff
                        logger.debug(f"Общий размер файлов 'fail' изменён: {old_fail} -> {self.size_fail}")
        return fileset_changed

    def remove_from_fileset(self, file:Path, extension:str, size:int, qc_pass:bool) -> bool:
        """
        Удаляет файл из сета файлов (в зависимости от расширения) 
        и соответствующего FileSubset.
        Возвращает True, если файл был удалён
        """
        file_removed: bool = False
        # выбираем сабсет на основе расширения файла
        if "fast5" in extension:
            file_removed = self.fast5.remove_file_from_subset(file, size, qc_pass)
            if file_removed:
                logger.debug(f"Удалён файл {file} из FileSubset 'fast5'")
        elif "pod5" in extension:
            file_removed = self.pod5.remove_file_from_subset(file, size, qc_pass)
            if file_removed:
                logger.debug(f"Удалён файл {file} из FileSubset 'pod5'")
        elif any([
                fq_ext in extension
                for fq_ext in ['fq', 'fastq']
                ]):
            file_removed = self.fq.remove_file_from_subset(file, size, qc_pass)
            if file_removed:
                logger.debug(f"Удалён файл {file} из FileSubset 'fq'")

        if file_removed:
            del self.files[file]
            # Удаляем размер файла из общего размера
            self.size_total -= size
            if qc_pass:
                self.size_pass -= size
                if extension in ['fast5', 'pod5']:
                    self.size_pass_sourcefiles -= size
            else:
                    self.size_fail -= size
        return file_removed


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
    def from_db(doc: Dict[str, Any]) -> SourceFileMeta:
        """
        Создаёт объект BatchMeta из документа БД.

        :param doc: Документ из коллекции 'batches' в MongoDB.
        :return: Объект BatchMeta.
        """
        # Инициализируем основные поля SourceFileMeta
        file_meta = SourceFileMeta(
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
        return file_meta

    def __post_init__(self):
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
        h = hashlib.blake2s()
        for v in (
                  self.size,
                  self.dev,
                  self.ino,
                  self.modified.timestamp()
                 ):
            h.update(str(v).encode())
            h.update(b'|')
        self.fingerprint = h.hexdigest()
        
    def finalize(self):
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


# Класс для хранения метаданных батчей
@dataclass(slots=True)
class BatchMeta:
    """
    Класс для хранения метаданных батчей
    """
    # Определяем при инициализации 
    name: str
    final_summary: Path
    sequencing_summary: Path
    created: Optional[datetime] = field(default=None)
    modified: Optional[datetime] = field(default=None)
    # Определяем по ходу наполнения батча файлами
    fingerprint: str = field(default_factory=str)
    files: FileSet = field(default_factory=FileSet)
    samples: Set[str] = field(default_factory=set)
    size: int = field(default=0)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib.blake2s = field(default_factory=hashlib.blake2s)
    # Курирование метаданных
    status: str = field(default_factory=str)

    # Список изменений в батче по сравнению с предыдущей версией (файл:изменения)
    changes: Dict[Path, Dict[str, str|int|datetime]] = field(default_factory=dict)
    # Отпечаток предыдущей версии батча (если есть)
    previous_version: str = field(default_factory=str)
    

    @staticmethod
    def from_db(doc: Dict[str, Any]) -> BatchMeta:
        """
        Создаёт объект BatchMeta из документа БД.

        :param doc: Документ из коллекции 'batches' в MongoDB.
        :return: Объект BatchMeta.
        """
        # Инициализируем основные поля BatchMeta
        batch = BatchMeta(
            name=doc.get("name", ""),
            final_summary=Path(doc.get("final_summary", "")),
            sequencing_summary=Path(doc.get("sequencing_summary", "")),
            created=doc.get("created"),
            modified=doc.get("modified"),
            fingerprint=doc.get("fingerprint", ""),
            samples=set(doc.get("samples", [])),
            status=doc.get("status", "indexed"),
            changes=doc.get("changes", {}),
            previous_version=doc.get("previous_version", ""),
        )

        if "files" in doc:
            batch.files = FileSet.from_db(doc["files"])

        return batch

    def update_batch(
                        self,
                        change_type:str,
                        file_meta: Optional[SourceFileMeta]=None,
                        file_meta_dict:Optional[Dict[str, str|int|datetime]]=None,
                        file_diffs:dict={}
                       ) -> bool:
        """
        Обновляет объект BatchMeta:
        - если файл был добавлен — проводит процедуру добавления меты файла в батч
        - если файл был удалён — удаляет его метаданные из батча
        - если файл был изменён — обновляет его метаданные в батче
        Возвращает True, если батч был изменён

        :param file_path: путь к файлу
        :param file_diffs: словарь с изменениями файла
        :param file_meta: объект SourceFileMeta
        :return: True, если батч был изменён
        """
        batch_changed: bool = False
        logger.debug(f"Обновление батча {self.name}")
        if file_meta:
            file_id = file_meta.symlink.as_posix()
            # Если файл был добавлен
            if change_type == "add":
                logger.debug(f"  Добавление метаданных файла {file_id}")
                batch_changed = self.add_file2batch(file_meta)
        elif file_meta_dict:
            file_id = Path(str(file_meta_dict['symlink']))
            filepath = Path(str(file_meta_dict['filepath']))
            # ...удалён...
            if change_type == "delete":
                logger.debug(f"  Удаление метаданных файла {file_id}")
                batch_changed =  self.remove_file_from_batch(file_meta_dict)
            # ...или модифицирован
            elif change_type == "modify":
                logger.debug(f"  Обновление метаданных файла {file_id}")
                batch_changed =  update_file_in_meta(self, filepath, file_diffs)
        return batch_changed

    def add_file2batch(self, src: SourceFileMeta) -> bool:
        """
        Добавляет SourceFileMeta в соответствующий поднабор (fast5/pod5 + pass/fail).
        Ожидается, что src.symlink уже установлен.
        """
        if self.files._file_added_to_fileset(
                                             src.symlink,
                                             src.fingerprint,
                                             src.extension,
                                             src.size,
                                             src.quality_pass
                                            ):
            # Добавляем образец, к которому относится файл, к списку образцов
            self.samples.add(src.sample)
            # Обновляем дату создания и последнего изменения батча
            self.created = min_datetime(self.created, src.created)
            self.modified = max_datetime(self.modified, src.modified)
            self.size += src.size
            # Обновляем отпечаток объекта, сохраняя в нём отпечаток добавленного файла
            self._fingerprint = update_fingerprint(
                                                   main_fingerprint=self._fingerprint,
                                                   fingerprint2add=src.fingerprint
                                                  )         
            return True
        else:
            return False

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        """
        Обновляет метаданные файла в батче.
        Возвращает True, если батч был изменён
        """
        batch_changed: bool = False
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            for prop, diffs in mod_data.items():
                old_val = diffs[0]
                new_val = diffs[1]
                if old_val != new_val:
                    batch_changed = True
                if prop == "size":
                    self.size += (new_val - old_val)
                elif prop == "modified":
                    if isinstance(new_val, datetime):
                        self.modified = max_datetime(self.modified, new_val)
                    else:
                        logger.error(f"Неверный тип данных для изменения {prop} в батче {self.name}")
                elif prop == "fingerprint":
                    if isinstance(new_val, str):
                        if not self._fingerprint:
                            self._fingerprint = hashlib.blake2s()
                            self._fingerprint.update(self.fingerprint.encode())
                        self._fingerprint = update_fingerprint(
                                                               main_fingerprint=self._fingerprint,
                                                               fingerprint2add=new_val
                                                              )
                    else:
                        logger.error(f"Неверный тип данных для изменения {prop} в батче {self.name}")
        return batch_changed

    def remove_file_from_batch(self, src: Dict[str, str|int|datetime]) -> bool:
        file_removed: bool = False

        meta_file_id = src['symlink']
        file_size:int = src['size'] # type: ignore
        if meta_file_id in self.files.files:
            logger.debug(f"Удаление файла {meta_file_id} из батча {self.name}...")
            file_removed = self.files.remove_from_fileset(
                                                          Path(str(meta_file_id)),
                                                          str(src['extension']),
                                                          file_size,
                                                          src['quality_pass'] # type: ignore
                                                         ) 
            if file_removed:
                self.size -= file_size
                self.modified = datetime.now()
                self.changes[Path(meta_file_id)] = {'status':'deleted'}
                logger.debug(f"Файл {meta_file_id} был удалён из батча {self.name}.")
                # Обновляем отпечаток, убирая рандомные пару букв сурса
                if not self._fingerprint:
                    self._fingerprint = hashlib.blake2s()
                    self._fingerprint.update(self.fingerprint.encode())
                self._fingerprint = update_fingerprint(
                                                       main_fingerprint=self._fingerprint,
                                                       fingerprint2add=src['fingerprint'][1:] # type: ignore
                                                      )
                # Проверяем, остались ли ещё файлы. Если нет - помечаем батч как устаревший
                if not self.files.files:
                    logger.debug(f"Батч {self.name} пуст. Меняем статус на 'deprecated'...")
                    self.status = 'deprecated'
        else:
            logger.error(f"Файл {meta_file_id} не найден в батче {self.name}!")
        return file_removed

    def finalize(self):
        self.fingerprint = generate_final_fingerprint(self._fingerprint)
        # помечаем батч как проиндексированный
        if not self.status:
            self.status = 'indexed'


# Класс для хранения метаданных образцов
@dataclass(slots=True)
class SampleMeta:
    """
    Класс для хранения метаданных образцов.
    """
    # Определяем при инициализации 
    name: str
    files: FileSet = field(default_factory=FileSet)
    # Наполняем после прохода по всем файлам
    batches: Set[str] = field(default_factory=set)
    size: int = 0
    # Определяем по метаданным батчей, в которых присутствует образец
    created: Optional[datetime] = field(default=None)
    modified: Optional[datetime] = field(default=None)
    # Определяем по ходу наполнения образца файлами
    fingerprint: str = field(default_factory=str)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib.blake2s = field(default_factory=hashlib.blake2s)
        
    # данные TaskScheduler
    # словарь вида {пайплайн: {id задания: статус}}
    tasks: Dict[str, Dict[str, str]] = field(default_factory=dict)
    
    # История изменений
    status: str = field(default_factory=str)
    # список изменений батчей и входящих в них файлов
    changes: Dict[str, Dict[Path, Dict[str, int | datetime]]] = field(default_factory=dict)
    # Отпечаток предыдущей версии батча (если есть)
    previous_version: str = field(default_factory=str)
    
    @staticmethod
    def from_db(doc: Dict[str, Any]) -> SampleMeta:
        """
        Создаёт объект SampleMeta из документа БД.

        :param doc: Документ из коллекции 'samples' в MongoDB.
        :return: Объект SampleMeta.
        """
        # Инициализируем основные поля SampleMeta
        sample = SampleMeta(
            name=doc.get("name", ""),
            batches=set(doc.get("batches", [])),
            size=doc.get("size", 0),
            created=doc.get("created"),
            modified=doc.get("modified"),
            fingerprint=doc.get("fingerprint", ""),
            status=doc.get("status", "indexed"),
            tasks=doc.get("tasks", {}),
            changes=doc.get("changes", {}),
            previous_version=doc.get("previous_version", ""),
        )

        if "files" in doc:
            sample.files = FileSet.from_db(doc["files"])
        
        # Восстанавливаем внутренний хэш-аккумулятор
        if "fingerprint" in doc:
            sample._fingerprint = hashlib.blake2s()
            sample._fingerprint = update_fingerprint(sample._fingerprint,
                                                     sample.fingerprint)
        
        return sample

    def update_sample(
                        self,
                        change_type:str,
                        batch_meta: BatchMeta,
                        file_meta: Optional[SourceFileMeta]=None,
                        file_meta_dict:Optional[Dict[str, str|int|datetime]]=None,
                        file_diffs:dict={}
                       ) -> bool:
        """
        Обновляет объект SampleMeta:
        - если файл был добавлен — проводит процедуру добавления меты файла в образец
        - если файл был удалён — удаляет его метаданные из образца
        - если файл был изменён — обновляет его метаданные в образце
        Возвращает True, если образец был изменён

        :param file_path: путь к файлу
        :param file_diffs: словарь с изменениями файла
        :param file_meta: объект SourceFileMeta
        :return: True, если образец был изменён
        """
        sample_changed: bool = False
        logger.debug(f"Обновление образца {self.name}")
        # Если файл был добавлен
        if file_meta:
            file_id = file_meta.symlink.as_posix()
            if change_type == "add":
                logger.debug(f"  Добавление метаданных файла {file_id}")
                # Проверяем наличие батча в образце, если его нет — добавляем
                sample_changed = self.add_batch2sample(
                                                       batch_meta=batch_meta,
                                                       file_meta=file_meta
                                                      )
                if sample_changed:
                    logger.debug(f"  Метаданные файла {file_id} добавлены в мету образца")
        elif file_meta_dict:
            file_id = str(file_meta_dict['symlink'])
            # ...удалён...
            if change_type == "delete":
                logger.debug(f"  Удаление метаданных файла {file_id}")
                sample_changed =  self.remove_file_from_sample(file_meta_dict)
            # ...или модифицирован
            elif change_type == "modify":
                logger.debug(f"  Обновление метаданных файла {file_id}")
                sample_changed =  update_file_in_meta(
                                                      self,
                                                      Path(str(file_meta_dict['filepath'])),
                                                      file_diffs
                                                     )
        return sample_changed

    def add_batch2sample(self,
                         batch_meta: BatchMeta,
                         file_meta: SourceFileMeta
                        ) -> bool:
        sample_changed: bool = False
        if batch_meta.name not in self.batches:
        # Обновляем даты последних изменения и создания батча
            self.created = min_datetime(self.created, batch_meta.created)
            self.modified = max_datetime(self.modified, batch_meta.modified)
            self.batches.add(batch_meta.name)
            sample_changed = True
        if self.add_file2sample(file_meta=file_meta):
            sample_changed = True
                    
        # Если до этого у сэмпла не было статуса - ставим 'indexed'
        if not self.status:
            self.status = 'indexed'
        return sample_changed        

    def add_file2sample(
                        self,
                        file_meta:SourceFileMeta
                       ) -> bool:
        """
        Добавляет файл в набор файлов.
        """

        # Добавляем, если файл не добавлен ранее
        if self.files._file_added_to_fileset(
                                             file_meta.symlink,
                                             file_meta.fingerprint,
                                             file_meta.extension,
                                             file_meta.size,
                                             file_meta.quality_pass
                                            ):
            self.size += file_meta.size
            self._fingerprint = update_fingerprint(
                                                   main_fingerprint=self._fingerprint,
                                                   fingerprint2add=file_meta.fingerprint
                                                  )
            return True
        return False

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        sample_changed: bool = False
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            for prop, diffs in mod_data.items():
                old_val = diffs[0]
                new_val = diffs[1]
                if old_val != new_val:
                    sample_changed = True
                if prop == "size":
                    self.size += (new_val - old_val)
                elif prop == "modified":
                    if isinstance(new_val, datetime):
                        self.modified = max_datetime(self.modified, new_val)
                    else:
                        logger.error(f"Неверный тип данных для изменения {prop} в образце {self.name}")
                elif prop == "fingerprint":
                    if isinstance(new_val, str):
                        self._fingerprint = update_fingerprint(
                                                              main_fingerprint=self._fingerprint,
                                                              fingerprint2add=new_val
                                                              )
                    else:
                        logger.error(f"Неверный тип данных для изменения {prop} в образце {self.name}")
        return sample_changed
    
    def remove_file_from_sample(
                                self,
                                file_meta_dict:Dict[str, str|int|datetime|bool]
                               ) -> bool:
        file_removed: bool = False
        if file_meta_dict:
            file_id = str(file_meta_dict['symlink'])
            file_size = int(file_meta_dict['size']) # type: ignore
            file_removed = self.files.remove_from_fileset(
                                                          Path(str(file_meta_dict['symlink'])),
                                                          str(file_meta_dict['extension']),
                                                          int(file_meta_dict["size"]), # type: ignore
                                                          file_meta_dict['quality_pass'] # type: ignore
                                                         )
            if file_removed:
                logger.debug(f"  Метаданных файла {file_id} удалены")
                self.size -= file_size
                self.modified = datetime.now()
                if not self._fingerprint:
                    self._fingerprint = hashlib.blake2s()
                    self._fingerprint = update_fingerprint(
                                                            self._fingerprint,
                                                            str(file_meta_dict['fingerprint'])[1:]
                                                            )
                    # Помечаем образец как неактуальный, если у него не осталось файлов И батчей на курации
                    if not self.files.files:
                        logger.debug(f"  Образец пуст")
                        self.status = 'deprecated'
            else:
                logger.error(f"Файл {file_id} не найден в образце {self.name}")
        return file_removed

    def finalize(self):
        self.fingerprint = generate_final_fingerprint(self._fingerprint)


def update_fingerprint(
                       main_fingerprint:hashlib.blake2s,
                       fingerprint2add:str
                       ) -> hashlib.blake2s:
    """
    Обновляет отпечаток объекта, добавляя строку другого отпечатка и разделитель
    """
    main_fingerprint.update(fingerprint2add.encode())
    main_fingerprint.update(b'|')
    return main_fingerprint


def generate_final_fingerprint(raw_fingerprint: hashlib.blake2s) -> str:
    """
    Создаёт строку отпечатка объекта на основе хэша
    """
    return raw_fingerprint.hexdigest()


def max_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    return b if a is None or (b and b > a) else a


def min_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    return b if a is None or (b and b < a) else a

def update_file_in_meta(
    meta_obj: Union[BatchMeta, SampleMeta],
    file_path: Path,
    diff: Dict[str, List[Any]]
) -> bool:
    """
    Обновляет структуры в BatchMeta/SampleMeta при изменении файла.
    Возвращает True, если какие-то метаданные были изменены.
    
    :param meta_obj: Объект BatchMeta или SampleMeta.
    :param file_id: Путь к изменённому файлу.
    :param diff: Словарь изменений {size: [old, new], modified: [old, new]}.
    :return: None
    """
    def _find_containing_structures(
                                    meta_obj: Union[BatchMeta, SampleMeta],
                                    file_path: Path
                                   ) -> List[BatchMeta|
                                             SampleMeta|
                                             FileSet|
                                             FileSubset|
                                             FileGroup]:
        """
        Находит все структуры в объекте, где присутствует файл.
        """
        subset:FileSubset
        filegroup:FileGroup
        structures:List[
                        BatchMeta|
                        SampleMeta|
                        FileSet|
                        FileSubset|
                        FileGroup
                       ]
        
        # добавляем сам объект в список структур
        structures = [meta_obj]
        # добавляем сам FileSet, проходим по FileSet, собирая все структуры, где файл присутствует
        structures.append(meta_obj.files) # type: ignore
        for filetype in ["fast5", "pod5", "fq"]:
            subset = getattr(meta_obj.files, filetype) # type: ignore
            for attr_name in ["pass_", "fail"]:
                filegroup = getattr(subset, attr_name)
                if file_path in filegroup.files:
                    structures.append(subset)
                    structures.append(subset.__getattribute__(attr_name))
        return structures

    # Находим все структуры, где файл присутствует
    containing_structures = _find_containing_structures(meta_obj, file_path) # type: ignore
    # обновляем их
    meta_changed = []
    for structure in containing_structures:
        meta_changed.append(structure.edit_file_meta({file_path: diff}))
        if meta_changed[-1]:
            logger.debug(f"Изменена структура: {type(structure)}")
    return any([f for f in meta_changed])
