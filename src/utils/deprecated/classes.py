from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
import hashlib
from utils.filesystem import load_yaml
from utils.nanopore import parse_fast5_pod5_metadata
import logging

pore_data = load_yaml(file_path='data/pores_n_chemistry.yaml', critical=True)
models = load_yaml(file_path='data/dorado_models.yaml', critical=True)
available_modifications = load_yaml(file_path='data/available_modifications.yaml', critical=True)
logger = logging.getLogger(__name__)  # наследует конфиг из watchdog.py

# Класс для хранения метаданных исходных файлов
@dataclass(slots=True)
class SourceFileMeta:
    filepath: Path
    extension: str = field(init=False)
    basename:str = field(init=False)
    quality_pass: bool = field(init=False)
    batch: str = field(init=False)
    sample: str = field(init=False)
    symlink: Path = field(init=False)
    size: int = field(init=False)
    ctime: datetime = field(init=False)
    mtime: datetime = field(init=False)
    nlink: int = field(init=False)
    fingerprint: str = field(init=False)

    def __post_init__(self):
        # Получаем данные по частям пути файла
        self.basename = self.filepath.name
        self.extension = '.'.join(self.filepath.suffixes).lower()
        self.quality_pass = True if '_pass_' in self.basename.lower() else False
        # Получаем данные по принадлежности файла
        self.batch = '_'.join(str(self.filepath.parts[-3]).split('_')[3:])
        self.sample = self.filepath.parts[-4]
        # Получаем метаданные файла 
        stat = self.filepath.stat()
        self.size = stat.st_size
        self.ctime = datetime.fromtimestamp(stat.st_ctime_ns)
        self.mtime = datetime.fromtimestamp(stat.st_mtime_ns)
        self.nlink = stat.st_nlink
        # Формируем отпечаток файла для отслеживания изменений
        h = hashlib.blake2s()
        for v in (
                  stat.st_size,
                  stat.st_ctime_ns,
                  stat.st_mtime_ns,
                  stat.st_nlink
                 ):
            h.update(str(v).encode())
            h.update(b'|')
        self.fingerprint = h.hexdigest()

    
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


@dataclass(slots=True)
class FileSubset:
    """
    Набор файлов одного типа (напр., fast5 или pod5):
        • size  — общий размер (pass + fail)
        • pass\_ — группа с файлами, прошедшими QC
        • fail  — группа с файлами, не прошедшими QC
    """
    size: int = 0
    pass_: FileGroup = field(default_factory=FileGroup)
    fail:  FileGroup = field(default_factory=FileGroup)

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
    fileset: Set[Path] = field(default_factory=set)
    fast5: FileSubset = field(default_factory=FileSubset)
    pod5: FileSubset = field(default_factory=FileSubset)
    fq: FileSubset = field(default_factory=FileSubset)

    def add2file_set(self, file: Path, extension: str, size: int, qc_pass: bool) -> bool:
        """
        Добавляет файл в сет файлов (в зависимости от расширения) 
        и соответствующий FileSubset, если он ещё не был добавлен.
        Возвращает True, если файл был добавлен
        """
        file_added: bool = False
        subset: FileSubset

        if file not in self.fileset:
            # выбираем сабсет на основе расширения файла
            if "fast5" in extension:
                subset = self.fast5
            elif "pod5" in extension:
                subset = self.pod5
            elif any([
                    fq_ext in extension
                    for fq_ext in ['fq', 'fastq']
                    ]):
                subset = self.fq
            else:
                return False
            
            file_added = subset.add2file_subset(file, size, qc_pass)
            
            if file_added:
                self.fileset.add(file)
                # Добавляем размер файла к общему размеру
                self.size_total += size
                if qc_pass:
                    self.size_pass += size
                else:
                    self.size_fail += size
        return file_added

#!!
@dataclass(slots=True)
class MoleculeData:
    fileset: Set[Path] = field(default_factory=set)
    r941: FileSet = field(default_factory=FileSet)
    r1041: FileSet = field(default_factory=FileSet)
    available_modifications: Dict[str, Dict[str, str]] = field(default_factory=dict)
    size_total: int = 0
    size_pass: int = 0
    # Счётчик для размера нормальных fast5/pod5
    size_pass_sourcefiles: int = 0
    size_fail: int = 0

    def add2molecule_set(self, file:Path, pore:str, extension:str, size:int, qc_pass:bool) -> bool:
        file_added: bool = False
        file_set: FileSet
        if file not in self.fileset:
            # выбираем сет на основе версии поры батча
            if pore == '941':
                file_set = self.r941
            elif pore == '1041':
                file_set = self.r1041
            else:
                logger.warning(f"Файл  {file}: Неизвестная версия поры '{pore}'")
            file_added = file_set.add2file_set(file=file, # pyright: ignore[reportPossiblyUnboundVariable]
                                               extension=extension,
                                               size=size,
                                               qc_pass=qc_pass)
            if file_added:
                self.fileset.add(file)
                # Добавляем размер файла к общему размеру
                self.size_total += size
                if qc_pass:
                    self.size_pass += size
                    # Добавляем размер файла к общему размеру файлов, подходящих для бейсколлинга
                    if extension in ['fast5', 'pod5']:
                        self.size_pass_sourcefiles =+ size
                else:
                    self.size_fail += size
        return file_added


# Класс для хранения метаданных батчей
@dataclass(slots=True)
class BatchMeta:
    """
    !!!
    """
    # Определяем при инициализации 
    name: str
    final_summary: Path 
    sequencing_summary: Path
    # Определяем через парсинг final_summary
    experiment_type: str = field(init=False)
    flow_cell: str = field(init=False)
    sequencing_kit: str = field(init=False)
    pore: str = field(init=False)
    #!!
    created: datetime = field(init=False)
    modified: datetime = field(init=False)
    # Определяем по ходу наполнения батча файлами
    fingerprint: str = field(init=False)
    files: FileSet = field(default_factory=FileSet)
    samples: Set[str] = field(default_factory=set)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib.blake2s = field(default_factory=hashlib.blake2s)
    # Курирование метаданных
    # В unknowns указываются неизвестные ячейка/химия
    metadata_observed: bool = field(default=False, init=False)
    needs_curation: bool = field(default=False, init=False)
    unknowns: Set[str] = field(default_factory=set)


    def add_file2batch(self, src: SourceFileMeta) -> None:
        """
        Добавляет SourceFileMeta в соответствующий поднабор (fast5/pod5 + pass/fail).
        Ожидается, что src.symlink уже установлен.
        """
        file_added:bool = False
        if src.symlink not in self.files.fileset:
            file_added = self.files.add2file_set(src.symlink, src.extension,
                                                 src.size, src.quality_pass)

        if file_added:
            src_file_metadata = {}
            val_unknown = 'unknown'
            
            # Добавляем образец, к которому относится файл, к списку образцов
            self.samples.add(src.sample)
            # Обновляем дату последнего изменения батча
            if self.modified:
                if src.mtime > self.modified:
                    self.modified = src.mtime
            else:
                self.modified = src.mtime
            # Обновляем отпечаток объекта, сохраняя в нём отпечаток добавленного файла
            self._fingerprint = update_fingerprint(
                                                  main_fingerprint=self._fingerprint,
                                                  fingerprint2add=src.fingerprint
                                                  )
           
            # Проверяем, не пора ли нам добрать метадату (есть хотя бы 3 файла на чтение и метадату еще не читали)
            if all([not self.metadata_observed, len(self.files.fileset)>2]):
                #Определяем 3 кандидатов для чтения метадаты по наименьшему размеру (важно при необходимости конвертации исходника)
                candidates = sorted(list(self.files.fileset), key=lambda f: f.stat().st_size)[:3]
                # Пытаемся отобрать метадату, пробуя набрать максимум данных. В случае наличия неизвестных пробуем следующего кандидата
                while candidates:
                    src_file = str(candidates.pop(0))
                    # Получаем данные из файла секвенирования
                    src_file_metadata = parse_fast5_pod5_metadata(src_file, val_unknown)
                    # Если метадата не все переменные определены - берём следующий образец
                    if val_unknown in src_file_metadata.values():
                        continue
                    else: break
                
                # Больше мы файлы батча для метадаты не трогаем    
                self.metadata_observed = True
                # Проверяем, что все данные в наличии, 
                # иначе - отправляем батч на курацию
                for file_param, batch_param in {
                                                'experiment_type':self.experiment_type,
                                                'sequencing_kit':self.sequencing_kit,
                                                'pore':self.pore,
                                                'flow_cell':self.flow_cell,
                                                'start_time':self.created
                                               }.items():
                    param_val = src_file_metadata[file_param]
                    if  param_val == val_unknown:
                        self.needs_curation = True
                        self.unknowns.add(file_param)
                    else:
                        if batch_param == self.created:
                            if batch_param:
                                if param_val < batch_param:
                                    batch_param = param_val
                            else:
                                batch_param = param_val
                            continue
                        batch_param = param_val


# Класс для хранения метаданных образцов
@dataclass(slots=True)
class SampleMeta:
    """
    !!!
    """
    # Определяем при инициализации 
    name: str
    fileset: Set[Path] = field(default_factory=set, init=False)
    # Наполняем после прохода по всем файлам
    batches: Set[str] = field(default_factory=set, init=False)
    dna : MoleculeData = field(default_factory=MoleculeData, init=False)
    rna : MoleculeData = field(default_factory=MoleculeData, init=False)
    size: int = 0
    # Определяем по метаданным батчей, в которых присутствует образец
    created: datetime = field(init=False)
    modified: datetime = field(init=False)
    # Определяем по ходу наполнения образца файлами
    fingerprint: str = field(init=False)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib.blake2s = field(default_factory=hashlib.blake2s, init=False)
    # Курирование метаданных
    needs_curation: bool = field(default=False, init=False)
    unknown_batches: Set[str] = field(default_factory=set, init=False)

    def add_batch2sample(self,
                         src: BatchMeta,
                         files_meta: Dict[str, SourceFileMeta]
                        ) -> None:
        # Обновляем даты последних изменения и создания батча
        if self.modified:
            if src.modified > self.modified:
                self.modified = src.modified
        else:
            self.modified = src.modified
        if self.created:
            if src.created < self.created:
                self.created = src.created
        else:
            self.created = src.created

        # Проверяем, нуждается ли батч в курации метаданных
        if src.needs_curation:
            self.needs_curation = True
            self.unknown_batches.add(src.name)
        else:
            self.batches.add(src.name)
            # Определяем сет, в который пойдёт файл
            if src.experiment_type=='dna':
                molecule_set = self.dna
            elif src.experiment_type=='rna':
                molecule_set = self.rna
            else:
                logger.warning(f"Батч  {src.name}: Неизвестный experiment_type '{src.experiment_type}'")
                molecule_set = False
            if molecule_set:
                sample_files = [file for file in src.files.fileset
                                if self.name in file.name]
                # Если все параметры батча на месте - добавляем файлы
                for file in sample_files:
                    file_meta = files_meta[str(file)]
                    if self.add_file2sample(file=file, batch_meta=src, file_meta=file_meta, molecule_set=molecule_set):
                        self.fileset.add(file)
                    
            self._fingerprint = update_fingerprint(
                                                  main_fingerprint=self._fingerprint,
                                                  fingerprint2add=src.fingerprint
                                                  )
        

    def add_file2sample(self,
                        batch_meta: BatchMeta,
                        file:Path,
                        file_meta:SourceFileMeta,
                        molecule_set:MoleculeData
                       ) -> bool:
        """
        Добавляет файл в общий набор файлов, а также в соответствующий поднабор файлов 
        версии поры, на которой он генерировался. При needs_curation=True
        файл не добавляется, зато имя батча добавляется в unknown_batches (с присвоением True
        параметру needs_curation)
        """
        file_added:bool = False
        # Проверяем, что файл не добавлен ранее
        if file not in self.fileset:
            file_added = molecule_set.add2molecule_set(file=file,
                                                       pore=batch_meta.pore,
                                                       extension=file_meta.extension,
                                                       size=file_meta.size,
                                                       qc_pass=file_meta.quality_pass)
            if file_added:
                self.fileset.add(file)
                self.size += file_meta.size
        return file_added


# ________________________
# Общие функции

def upsert_batch(
                 batches: Dict[str, BatchMeta],
                 src: SourceFileMeta,
                 summaries:dict
                 ) -> Dict[str, BatchMeta]:
    """
    Обеспечивает уникальность BatchMeta по имени и добавляет туда SourceFileMeta.
    Если батча с таким name нет — создаёт; если есть — переиспользует и дополняет.

    :param batches: словарь существующих батчей по имени
    :param src: объект SourceFileMeta (должны быть заполнены: batch, extension, size, symlink, quality_pass)
    :return: актуальный объект BatchMeta
    """
    name = src.batch
    bm = batches.get(name)
    if bm is None:
        bm = BatchMeta(
                       name=name,
                       final_summary=next(s for s
                                          in summaries['final_summary']
                                          if name in s),
                       sequencing_summary=next(s for s
                                               in summaries['sequencing_summary']
                                               if name in s)
                       )
    # добавляем файл в fast5/pod5 + pass/fail
    bm.add_file2batch(src)
    # Обновляем батч в списке батчей
    batches[name] = bm
    return batches


def upsert_samples(
                 samples: Dict[str, SampleMeta],
                 src: BatchMeta,
                 files_meta: Dict[str, SourceFileMeta]
                 ) -> Dict[str, SampleMeta]:
    """
    Обеспечивает уникальность SampleMeta по имени и добавляет туда информацию по файлам.
    Если образца с таким именем нет — создаёт; если есть — переиспользует и дополняет.

    :param batches: словарь существующих батчей по имени
    :param src: объект SourceFileMeta (должны быть заполнены: batch, extension, size, symlink, quality_pass)
    :return: актуальный объект BatchMeta
    """
    for sample_name in src.samples:
        sm = samples.get(sample_name)
        if sm is None:
            sm = SampleMeta(name=sample_name)
            sm.add_batch2sample(src, files_meta)
        else:
            sm.add_batch2sample(src, files_meta)
        samples[sample_name] = sm
    return samples


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