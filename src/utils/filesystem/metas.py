# -*- coding: utf-8 -*-
"""
Файловая система: индексация исходников, создание симлинков и базовая мета
"""
from __future__ import annotations
import pod5
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from time import time, process_time
from datetime import datetime, timezone
import subprocess
from utils.refs import REFS
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from utils.db import ConfigurableMongoDAO
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
            self.size += size
            file_added = True
        return file_added


    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> None:
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            size_diffs = mod_data.get('size')
            if size_diffs:
                self.size += (size_diffs[1] - size_diffs[0])


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
            file_added = self.pass_.add2file_group(file, size)
        else:
            file_added = self.fail.add2file_group(file, size)
        if file_added:
            self.size += size
        return file_added
        

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> None:
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            size_diffs = mod_data.get('size')
            if size_diffs:
                self.size += (size_diffs[1] - size_diffs[0])


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
                file_added = self.fast5.add2file_subset(file, size, qc_pass)
            elif "pod5" in extension:
                file_added = self.pod5.add2file_subset(file, size, qc_pass)
            elif any([
                    fq_ext in extension
                    for fq_ext in ['fq', 'fastq']
                    ]):
                file_added = self.fq.add2file_subset(file, size, qc_pass)
            else:
                return False

            if file_added:
                self.files[file] = fingerprint
                # Добавляем размер файла к общему размеру
                self.size_total += size
                if qc_pass:
                    self.size_pass += size
                    if extension in ['fast5', 'pod5']:
                        self.size_pass_sourcefiles =+ size
                else:
                    self.size_fail += size
        return file_added


    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> None:
        # проходим по словарям изменений
        for file, mod_data in edit_dict.items():
            size_diffs = mod_data.get('size')
            if size_diffs:
                old_val = size_diffs[0]
                new_val = size_diffs[1]
                size2add = new_val - old_val
                self.size_total += size2add
                if 'pass' in file.name:
                    self.size_pass += size2add
                    if file.suffix in ['.fast5', '.pod5']:
                        self.size_pass_sourcefiles += size2add
                else:
                    self.size_fail += size2add


@dataclass(slots=True)
class PoreSet:
    _molecule_type:str = field(default_factory=str)
    _pore_version: str = field(default_factory=str)
    files: FileSet = field(default_factory=FileSet)
    dorado_container: str = field(default="")
    available_modifications: List[str] = field(default_factory=list)
    refs_version: str = field(default="")
    

    def __post_init__(self):
        data = pore_data[self._pore_version]
        self.dorado_container = data['dorado_container']
        self.available_modifications = data['available_modifications'][self._molecule_type]
        self.refs_version = refs_version['pore_data']


    @staticmethod
    def from_db(doc: Dict[str, Any],
                molecule_type:str,
                pore_version:str
               ) -> PoreSet:
        return PoreSet(_molecule_type=molecule_type,
                       _pore_version=pore_version,
                       files=FileSet.from_db(doc.get("files", {})),
                       dorado_container=doc.get("dorado_container", ""),
                       available_modifications=doc.get("available_modifications", []),
                       refs_version=doc.get("refs_version", ""))        


@dataclass
class MoleculeData:
    files: Set[Path] = field(default_factory=set)
    pores: Dict[str, PoreSet] = field(default_factory=dict)
    size_total: int = 0
    size_pass: int = 0
    # Счётчик для размера нормальных fast5/pod5
    size_pass_sourcefiles: int = 0
    size_fail: int = 0


    @staticmethod
    def from_db(doc: Dict[str, Any], molecule_type:str) -> MoleculeData:
        """
        Восстанавливает объект MoleculeData из документа БД.
        """
        return MoleculeData(
                            files=doc.get("files", set),
                            pores={pore_name:PoreSet.from_db(
                                                             doc=pore_doc,
                                                             molecule_type=molecule_type,
                                                             pore_version=pore_name
                                                            )
                                   for pore_name, pore_doc in doc.get("pores", {}).items()},
                            size_total=doc.get("size_total", 0),
                            size_pass=doc.get("size_pass", 0),
                            size_pass_sourcefiles=doc.get("size_pass_sourcefiles", 0),
                            size_fail=doc.get("size_fail", 0)
                           )


    def add2molecule_set(self, file:Path, fingerprint:str, molecule:str, pore:str, extension:str, size:int, qc_pass:bool) -> bool:
        file_added: bool = False

        if file not in self.files:
            # Создаём на основе поры отдельный PoreSet
            if pore:
                if pore not in self.pores.keys():
                    self.pores[pore] = PoreSet(molecule, pore)
                file_added = self.pores[pore].files.add2file_set(file, fingerprint, extension, size, qc_pass)
                if file_added:
                    self.files.add(file)
                    # Добавляем размер файла к общему размеру
                    self.size_total += size
                    if qc_pass:
                        self.size_pass += size
                        # Добавляем размер файла к общему размеру файлов, подходящих для бейсколлинга
                        if extension in ['fast5', 'pod5']:
                            self.size_pass_sourcefiles += size
                    else:
                        self.size_fail += size
            else:
                logger.warning(f"Файл  {file}: Неизвестная версия поры '{pore}'")
        return file_added


    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> None:
        # проходим по словарям изменений
        for file, mod_data in edit_dict.items():
            size_diffs = mod_data.get('size')
            if size_diffs:
                old_val = size_diffs[0]
                new_val = size_diffs[1]
                size2add = new_val - old_val
                self.size_total += size2add
                if 'pass' in file.name:
                    self.size_pass += size2add
                    if file.suffix in ['.fast5', '.pod5']:
                        self.size_pass_sourcefiles += size2add
                else:
                    self.size_fail += size2add


# Класс для хранения метаданных исходных файлов
@dataclass(slots=True)
class SourceFileMeta:
    name: str  = field(init=False)
    filepath: Path
    directory: Path = field(init=False)
    symlink_dir: Path
    symlink: Path = field(init=False)
    extension: str = field(init=False)
    basename:str = field(init=False)
    quality_pass: bool = field(init=False)
    batch: str = field(init=False)
    sample: str = field(init=False)
    size: int = field(init=False)
    created: datetime = field(init=False)
    modified: datetime = field(init=False)
    dev: int = field(init=False)
    ino:  int = field(init=False)
    nlink: int = field(init=False)
    status: str = field(init=False)
    # Список изменений в файле по сравнению с предыдущей версией (если есть)
    changes: Dict[str, int | datetime] = field(init=False)
    # Отпечаток предыдущей версии файла (если есть)
    previous_version: str = field(init=False)
    fingerprint: str = field(init=False)

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
        # Получаем данные по частям пути файла
        # Имя файла без директорий и расширения
        self.basename = self.filepath.name.split('.')[0]
        self.directory = self.filepath.parent
        raw_extensions = self.filepath.suffixes
        # Расширение файла без точек и .gz 
        self.extension = next(f.lower().removeprefix('.')
                               for f in raw_extensions
                               if f in ['.fast5', '.fastq', '.fq', '.pod5'])
        self.quality_pass = True if '_pass_' in self.basename.lower() else False
        self.name = self.symlink.name
        self.status = 'indexed'


# Класс для хранения метаданных батчей
@dataclass(slots=True)
class BatchMeta:
    # Определяем при инициализации 
    name: str
    final_summary: Path
    sequencing_summary: Path
    # Определяем через парсинг fast5/pod5
    meta_gathered_from: List[str] = field(default_factory=list)
    experiment_type: str = field(default_factory=str)
    flow_cell: str = field(default_factory=str)
    sequencing_kit: str = field(default_factory=str)
    pore: str = field(default_factory=str)
    created: Optional[datetime] = field(default=None)
    modified: Optional[datetime] = field(default=None)
    #created: Optional[datetime] = field(default=None, init=False)
    #modified: Optional[datetime] = field(default=None, init=False)
    # Определяем по ходу наполнения батча файлами
    fingerprint: str = field(default_factory=str)
    files: FileSet = field(default_factory=FileSet)
    samples: Set[str] = field(default_factory=set)
    size: int = field(default=0)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib.blake2s = field(default_factory=hashlib.blake2s)
    # Курирование метаданных
    status: str = field(default_factory=str)
    # В unknowns указываются неизвестные ячейка/химия
    metadata_observed: bool = field(default=False)
    unknowns: Dict[str, str] = field(default_factory=dict)
    # Сразу проверяем, что в data/pore_data.yaml есть нужные данные по поре/молекуле
    refs_version: str = field(default_factory=str)
    # Список изменений в батче по сравнению с предыдущей версией (файл:изменения)
    changes: Dict[Path, Dict[str, int | datetime]] = field(default_factory=dict)
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
            meta_gathered_from=doc.get("meta_gathered_from", []),
            experiment_type=doc.get("experiment_type", ""),
            flow_cell=doc.get("flow_cell", ""),
            sequencing_kit=doc.get("sequencing_kit", ""),
            pore=doc.get("pore", ""),
            created=doc.get("created"),
            modified=doc.get("modified"),
            fingerprint=doc.get("fingerprint", ""),
            samples=set(doc.get("samples", [])),
            status=doc.get("status", "indexed"),
            metadata_observed=doc.get("metadata_observed", False),
            unknowns=doc.get("unknowns", {}),
            refs_version=doc.get("refs_version", ""),
            changes=doc.get("changes", {}),
            previous_version=doc.get("previous_version", ""),
        )

        if "files" in doc:
            batch.files = FileSet.from_db(doc["files"])

        return batch



    def add_file2batch(self, src: SourceFileMeta) -> None:
        """
        Добавляет SourceFileMeta в соответствующий поднабор (fast5/pod5 + pass/fail).
        Ожидается, что src.symlink уже установлен.
        """
        file_added:bool = False
        if src.symlink not in self.files.files:
            file_added = self.files.add2file_set(src.symlink, src.fingerprint, src.extension,
                                                 src.size, src.quality_pass)

        if file_added:
            src_file_metadata = {}
            val_unknown = 'unknown'
            
            # Добавляем образец, к которому относится файл, к списку образцов
            self.samples.add(src.sample)
            # Обновляем дату последнего изменения батча
            self.modified = max_datetime(self.modified, src.modified)
            self.size += src.size
            # Обновляем отпечаток объекта, сохраняя в нём отпечаток добавленного файла
            self._fingerprint = update_fingerprint(
                                                  main_fingerprint=self._fingerprint,
                                                  fingerprint2add=src.fingerprint
                                                  )
           
            # Проверяем, не пора ли нам добрать метадату (есть хотя бы 3 файла на чтение и метадату еще не читали)
            if all([not self.metadata_observed, len(self.files.files)>2]):
                #Определяем 3 кандидатов для чтения метадаты по наименьшему размеру (важно при необходимости конвертации исходника)
                candidates = sorted(list(self.files.files), key=lambda f: f.stat().st_size)[:3]
                # Пытаемся отобрать метадату, пробуя набрать максимум данных. В случае наличия неизвестных пробуем следующего кандидата
                for src_file in candidates:
                    # сразу отметим файл как источник метаданных
                    self.meta_gathered_from.append(src_file.as_posix())
                    # Получаем данные из файла секвенирования
                    try:
                        src_file_metadata = parse_fast5_pod5_metadata(src_file.as_posix(), val_unknown)
                        # Если в метадате не все переменные определены - берём следующий образец
                        if val_unknown in src_file_metadata.values():
                            continue
                        else: break
                    # Ловим ситуацию, когда файл куда-то делся
                    except Exception:
                        print(f"Trouble reading file: {src_file}")
                
                # Больше мы файлы батча для метадаты не трогаем    
                self.metadata_observed = True
                # Проверяем, что все данные в наличии, 
                # особенно - версия поры и тип эксперимента.
                # При отсутствии этих двух - отправляем батч на курацию
                for meta_key in ['experiment_type',
                                 'sequencing_kit',
                                 'pore',
                                 'flow_cell',
                                 'created']:
                    param_val = src_file_metadata[meta_key]
                    
                    # Проводим проверки меты, важной для будущей обработки
                    if meta_key in ['experiment_type', 'pore']:
                        if any([param_val == val_unknown, # значения должны быть известны
                                (meta_key == 'pore' and param_val not in pore_data.keys()), # для поры есть референсные данные
                                (meta_key == 'experiment_type' and param_val not in   # нужная молекула указана в данных поры
                                             pore_data.get(src_file_metadata['pore'], {}).keys())
                                ]):
                            self.status = 'curation'
                            self.unknowns[meta_key] = param_val

                    elif meta_key == 'created':
                        if isinstance(param_val, datetime):
                            self.created = min_datetime(self.created, param_val)
                        continue
                    setattr(self, meta_key, param_val)


    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> None:
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            for prop, diffs in mod_data.items():
                if prop == 'deleted':
                    continue
                old_val = diffs[0]
                new_val = diffs[1]
                if prop == "size":
                    self.size += (new_val - old_val)
                elif prop == "modified":
                    if isinstance(new_val, datetime):
                        self.modified = max_datetime(self.modified, new_val)
                    else:
                        logger.error(f"Неверный тип данных для изменения {prop} в батче {self.name}")
                elif prop == "fingerprint":
                    if isinstance(new_val, str):
                        self._fingerprint = update_fingerprint(
                                                              main_fingerprint=self._fingerprint,
                                                              fingerprint2add=new_val
                                                              )
                    else:
                        logger.error(f"Неверный тип данных для изменения {prop} в батче {self.name}")
                

    """# если раньше были unknowns, а версия обновилась — можно триггерить пересчёт на следующем цикле
    def needs_reconcile(self, current_refs_version: str) -> bool:
        return all([
                   self.needs_curation,
                   self.refs_version != current_refs_version
                  ])"""
    def update_batch(
                        self,
                        file_path:Path,
                        file_diffs:dict,
                        file_meta: Optional[SourceFileMeta] = None 
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
        logger.debug(f"Обновление батча {self.name}")
        # Если файл был добавлен
        if file_meta:
            logger.debug(f"  Добавление метаданных файла {file_meta.name}")
            self.add_file2batch(file_meta)
            return True
        # Если файл был изменён:
        # ...удалён...
        if file_diffs.get('deleted', False):
            logger.debug(f"  Удаление метаданных файла {file_path.as_posix()}")
            pass
            return True
        # ...или модифицирован
        else:
            logger.debug(f"  Обновление метаданных файла {file_path.as_posix()}")
            update_file_in_meta(self, file_path, file_diffs)


    def finalize(self):
        self.fingerprint = generate_final_fingerprint(self._fingerprint)
        # Если до этого не поймали 'curation', помечаем батч как проиндексированный
        if not self.status:
            self.status = 'indexed'


# Класс для хранения метаданных образцов
@dataclass(slots=True)
class SampleMeta:
    """
    !!!
    """
    # Определяем при инициализации 
    name: str
    files: Set[Path] = field(default_factory=set)
    # Наполняем после прохода по всем файлам
    batches: Set[str] = field(default_factory=set)
    batches_unknown: Set[str] = field(default_factory=set)
    dna : MoleculeData = field(default_factory=MoleculeData)
    rna : MoleculeData = field(default_factory=MoleculeData)
    size: int = 0
    # Определяем по метаданным батчей, в которых присутствует образец
    created: Optional[datetime] = field(default=None)
    modified: Optional[datetime] = field(default=None)
    # Определяем по ходу наполнения образца файлами
    fingerprint: str = field(default_factory=str)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib.blake2s = field(default_factory=hashlib.blake2s)
    # Курирование метаданных
    status: str = field(default_factory=str)
    # сохраняем хэш референсов для триггера обновления данных
    refs_version: str = field(default_factory=str)


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
            files={Path(f) for f in doc.get("files", [])},
            batches=set(doc.get("batches", [])),
            batches_unknown=set(doc.get("batches_unknown", [])),
            dna=MoleculeData.from_db(doc, "dna"),
            rna=MoleculeData.from_db(doc, "rna"),
            size=doc.get("size", 0),
            created=doc.get("created"),
            modified=doc.get("modified"),
            fingerprint=doc.get("fingerprint", ""),
            status=doc.get("status", "indexed"),
            refs_version=doc.get("refs_version", "")
        )
        
        # Восстанавливаем внутренний хэш-аккумулятор
        if "fingerprint" in doc:
            sample._fingerprint = hashlib.blake2s()
            sample._fingerprint = update_fingerprint(sample._fingerprint,
                                                     sample.fingerprint)
        
        return sample


    def add_batch2sample(self,
                         src: BatchMeta,
                         files_meta: Dict[str, SourceFileMeta]
                        ) -> None:
        # Обновляем даты последних изменения и создания батча
        self.created = min_datetime(self.created, src.created)
        self.modified = max_datetime(self.modified, src.modified)

        self.batches.add(src.name)
        # Проверяем, нуждается ли батч в курации метаданных
        if src.status == 'curation':
            self.status = 'curation'
            self.batches_unknown.add(src.name)
        else:
            # Определяем сет, в который пойдут файлы батча
            if src.experiment_type=='dna':
                molecule_set = self.dna
            elif src.experiment_type=='rna':
                molecule_set = self.rna
            else:
                logger.warning(f"Батч {src.name}: Неизвестный experiment_type '{src.experiment_type}'")
                molecule_set = False
            if molecule_set:
                sample_files = [file for file in src.files.files
                                if self.name in file.name]
                # Если все параметры батча на месте - добавляем файлы
                for file in sample_files:
                    file_meta = files_meta[file.as_posix()]
                    if self.add_file2sample(file=file, batch_meta=src, file_meta=file_meta, molecule_set=molecule_set):
                        self.files.add(file)
                    
        self._fingerprint = update_fingerprint(
                                                main_fingerprint=self._fingerprint,
                                                fingerprint2add=src.fingerprint
                                                )
        # Если до этого у сэмпла не было статуса - ставим 'indexed'
        if not self.status:
            self.status = 'indexed'
        

    def add_file2sample(self,
                        batch_meta: BatchMeta,
                        file:Path,
                        file_meta:SourceFileMeta,
                        molecule_set:MoleculeData
                       ) -> bool:
        """
        Добавляет файл в общий набор файлов, а также в соответствующий поднабор файлов 
        типа экспериментов, на котором он генерировался. При needs_curation=True
        файл не добавляется, зато имя батча добавляется в batches_unknown (с присвоением True
        параметру needs_curation)
        """
        file_added:bool = False
        # Проверяем, что файл не добавлен ранее
        if file not in self.files:
            file_added = molecule_set.add2molecule_set(file=file,
                                                       fingerprint=file_meta.fingerprint,
                                                       molecule=batch_meta.experiment_type,
                                                       pore=batch_meta.pore,
                                                       extension=file_meta.extension,
                                                       size=file_meta.size,
                                                       qc_pass=file_meta.quality_pass)
            if file_added:
                self.files.add(file)
                self.size += file_meta.size
        return file_added


    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> None:
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            for prop, diffs in mod_data.items():
                if prop == 'deleted':
                    continue
                old_val = diffs[0]
                new_val = diffs[1]
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
  


    def finalize(self):
        self.refs_version = refs_version['pore_data']
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


def parse_fast5_pod5_metadata(source_file:str, val_unknown:str) -> dict:
    """
    Ищет в файле метаданные: тип эксперимента, частоту считывания, кит, версия поры, скорость прогонки ч/з пору, имя ячейки, тип секвенатора.\n
    При необходимости конвертирует fast5>pod5, возвращает словарь с метаданными
    """
    pod5_file: str = ""
    metadata_keys = ['created', 'sample_frequency', 'sequencing_kit', 'experiment_type',
                     'pore', 'pore_speed', 'flow_cell', 'sequencer_type']
    metadata: dict = {k: val_unknown
                      for k in metadata_keys}
    
    if source_file.endswith('.pod5'):
        pod5_file = source_file
        metadata = read_pod5(pod5_file, metadata) # pyright: ignore[reportUnboundVariable]
        if metadata['sequencing_kit'] != val_unknown:
                metadata['sequencing_kit'] = metadata['sequencing_kit'].upper()
        if not metadata:
            logger.warning(f'Файл {source_file} не содержит метаданных!')
        return metadata
    elif source_file.endswith('.fast5'):
        pod5_file = '/tmp/tmp.pod5'
        run_shell_cmd(cmd=f'pod5 convert fast5 {source_file} --output {pod5_file} --threads 4')
        try:
            metadata = read_pod5(pod5_file, metadata) # pyright: ignore[reportUnboundVariable]
            if not metadata:
                logger.warning(f'Файл {source_file} не содержит метаданных!')
        except KeyboardInterrupt as e:
            print('Прервано пользователем')
        finally:
            run_shell_cmd(cmd=f'rm {pod5_file}')
            return metadata
    else:        
        return metadata


def read_pod5(pod5_file:str, metadata:dict) -> dict:
    with pod5.Reader(pod5_file) as reader:
        try:
            # Берём первый рид, читаем его свойства
            read = next(reader.reads())
        except StopIteration:
            return metadata
        # Собираем метадату по свойствам read.run_info, в случае её отсутствия - пишем 'unknown'
        if read.run_info.acquisition_start_time:
            metadata['created'] = read.run_info.acquisition_start_time
        if read.run_info.context_tags:
            for key in ['sample_frequency', 'sequencing_kit']:
                if key in read.run_info.context_tags.keys():
                    metadata[key] = read.run_info.context_tags[key]
            # Вытаскиваем basecall_config_filename для извлечения данных о поре и скорости чтения
            basecall_config_filename = read.run_info.context_tags.get('basecall_config_filename', '')
            if basecall_config_filename:
                bcf_parts = basecall_config_filename.split('_')
                metadata['experiment_type'] = bcf_parts[0]
                metadata['pore'] = bcf_parts[1].replace('.', '')
                metadata['pore_speed'] = bcf_parts[2]  
        
        for k,v in {'flow_cell':read.run_info.flow_cell_product_code,
                    'sequencer_type':read.run_info.sequencer_position_type}.items():
            if v:
                metadata[k] = v
    return metadata


def run_shell_cmd(cmd:str, timeout:int=0, debug:str='') -> dict:
    """
    Выполняет shell-команду с таймаутом и логированием результатов.

    :param cmd: Команда для выполнения (строка, которая будет передана в shell)
    :param timeout: Максимальное время выполнения в секундах (0 - без таймаута)
    :param debug: Уровень логирования ('errors' - только ошибки, 'info' - stdout, 'all' - всё)
    :returns: Словарь с результатами выполнения:
        - log: метаданные выполнения (статус, временные отметки, код возврата)
        - stderr: содержимое stderr (если есть)
        - stdout: содержимое stdout (если есть)
    """

    timeout_param = None if timeout == 0 else timeout
    # Время начала (общее)
    start_time = time()
    cpu_start_time = process_time()
    start_datetime = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    stdout, stderr = "", ""

    result = subprocess.Popen(args=cmd,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              universal_newlines=True,
                              executable="/bin/bash",
                              bufsize=1,
                              cwd=None,
                              env=None)
    
    try:       
        # Ожидаем завершения с таймаутом
        stdout, stderr = result.communicate(timeout=timeout_param)
        # Построчно читаем стандартный вывод и ошибки в зависимости от уровня дебага
        """
        if debug:
            streams = []
            if debug in ['errors', 'all']:
                streams.append(('STDERR', stderr.splitlines()))
            if debug in ['info', 'all']:
                streams.append(('STDOUT', stdout.splitlines()))

            for label, stream in streams:
                for line in stream:
                    print(f"{label}: {line.strip()}")
        """            
        if debug:
            if debug in ['errors', 'all'] and stderr:
                for line in stderr.splitlines():
                    logger.error(f"STDERR: {line.strip()}")
            if debug in ['info', 'all'] and stdout:
                for line in stdout.splitlines():
                    logger.info(f"STDOUT: {line.strip()}")

        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)

        if result.returncode == 0:
            logger.info(f"Команда '{cmd}' выполнена успешно.")
        else:
            logger.warning(f"Команда:\n '{cmd}' \nзавершилась с ошибкой (код {result.returncode}).")

        # Лог выполнения случае завершения без исключений
        return {
            'log': {
                'status': 'OK' if result.returncode == 0 else 'FAIL',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': result.returncode
            },
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''
        }

    except subprocess.TimeoutExpired:
        result.kill()
        stdout, stderr = result.communicate()
        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)
        # Лог при тайм-ауте
        logger.error(f"Команда:\n '{cmd}' \nпревысила таймаут ({timeout}s).")
        return {
            'log': {
                'status': 'FAIL',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': "TIMEOUT"
            },
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''
        }
    # Прерывание пользователем
    except KeyboardInterrupt:
        result.kill()
        print('INTERRUPTED')
        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)
        logger.error(f"Команда:\n '{cmd}' \nпрервана пользователем (KeyboardInterrupt).")
        return {
            'log':
                {'status': 'FAIL',
                'start_time':start_datetime,
                'end_time':end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': 'INTERRUPTED'},
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''   
                }


def get_duration(duration_sec:float=0, start_time:float=0, cpu_start_time:float=0, precision:str='s') -> tuple:
    """
    Возвращает продолжительность выполнения команды в необходимом формате.

    :param duration_sec: Предварительно рассчитанная длительность в секундах (если 0 - вычисляется)
    :param start_time: Время начала выполнения (timestamp)
    :param cpu_start_time: CPU-время начала выполнения (process_time)
    :param precision: Точность форматирования ('d'-дни, 'h'-часы, 'm'-минуты, 's'-секунды)
    :returns: Кортеж с:
        - duration_sec: общая длительность в секундах
        - time_str: отформатированная строка времени
        - cpu_duration: CPU-время выполнения
        - end_datetime: строка с датой/временем завершения
    """

    # Время завершения (общее)
    duration_sec = int(time() - start_time)
    
    # Форматируем секунды в дни, часы и минуты
    time_str = convert_secs_to_dhms(secs=duration_sec, precision=precision)

    cpu_duration = process_time() - cpu_start_time
    end_datetime = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    return (duration_sec, time_str, cpu_duration, end_datetime)


def convert_secs_to_dhms(secs:int, precision:str='s') -> str:
    """
    Конвертирует секунды в удобочитаемый формат (дни/часы/минуты/секунды).

    :param secs: Количество секунд для конвертации
    :param precision: Максимальная единица точности ('d','h','m','s')
    :returns: Отформатированная строка времени (например "1h 30m")
    :raises ValueError: Если указана недопустимая точность
    """

    # Форматируем секунды в дни, часы и минуты
    if precision not in ['d', 'h', 'm', 's']:
        raise ValueError("Неправильное указание уровня точности!")
    d, not_d = divmod(secs, 86400) # Возвращает кортеж из целого частного и остатка деления первого числа на второе
    h, not_h = divmod(not_d, 3600)
    m, s = divmod(not_h, 60)
    measures = {'d':d, 'h':h, 'm':m, 's':s}
    to_string = []
    # Разряд времени пойдет в результат, если его значение не 0
    for measure, val in measures.items():
        if val !=0:
            to_string.append(f'{val}{measure}')
        if measure == precision:
            break
    # Формируем строку и определяем уровни точности
    time_str = " ".join(to_string)
    if len(time_str) == 0:
        time_str = (f'< 1{precision}')
    return time_str


def update_file_in_meta(
    meta_obj: Union[BatchMeta, SampleMeta],
    file_path: Path,
    diff: Dict[str, List[Any]]
) -> None:
    """
    Обновляет структуры в BatchMeta/SampleMeta при изменении файла.
    
    :param meta_obj: Объект BatchMeta или SampleMeta.
    :param file_id: Путь к изменённому файлу.
    :param diff: Словарь изменений {size: [old, new], modified: [old, new]}.
    :return: None
    """
    # Находим все структуры, где файл присутствует
    containing_structures = _find_containing_structures(meta_obj, file_path) # type: ignore
    # обновляем их
    for structure in containing_structures:
        structure.edit_file_meta({file_path: diff})
    # !!! дальше пишем функции edit для всех структур !!!

    def _find_containing_structures(
                                    meta_obj: Union[BatchMeta, SampleMeta],
                                    file_path: Path
                                   ) -> List[Any]:
        """
        Находит все структуры в объекте, где присутствует файл.
        """
        mol_data: MoleculeData
        fileset_containing_structure:Union[BatchMeta, PoreSet]
        subset:FileSubset
        filegroup:FileGroup
        structures:List[Union[BatchMeta,
                              SampleMeta,
                              MoleculeData,
                              FileSet,
                              FileSubset,
                              FileGroup]] = []
        
        # добавляем сам объект в список структур
        structures.append(meta_obj)
        # спускаемся по дереву атрибутов к объекту FileSet, содержащему файл (для BatchMeta и SampleMeta спуск разный)
        if isinstance(meta_obj, BatchMeta):
            fileset_containing_structure =  meta_obj
        elif isinstance(meta_obj, SampleMeta):
            for molecule in ["dna", "rna"]:
                if hasattr(meta_obj, molecule):
                    mol_data = getattr(meta_obj, molecule)
                    if file_path in mol_data.files:
                        structures.append(mol_data)
                        for pore in mol_data.pores.keys():
                            if file_path in mol_data.pores[pore].files.files.keys():
                                fileset_containing_structure = mol_data.pores[pore]
        # добавляем сам FileSet, проходим по FileSet, собирая все структуры, где файл присутствует
        structures.append(fileset_containing_structure.files) # type: ignore
        for filetype in ["fast5", "pod5", "fq"]:
            subset = getattr(fileset_containing_structure.files, filetype) # type: ignore
            for attr_name in ["pass_", "fail"]:
                filegroup = getattr(subset, attr_name)
                if file_path in filegroup.files:
                    structures.append(subset)
                    structures.append(subset.__getattribute__(attr_name))
        return structures
