import pod5
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import  Dict, List, Set, Optional
from utils.common import run_shell_cmd, log_error, min_datetime, max_datetime, load_yaml
from utils.refs import REFS

logger = logging.getLogger(__name__)  # наследует конфиг из watchdog.py
pore_data, models, avail_mods, refs_version = REFS.get()

""""
________________________
Классы
________________________
"""
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
                return file_added
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
                        self.size_pass_sourcefiles += size
                else:
                    self.size_fail += size
        return file_added


# Класс для хранения метаданных исходных файлов
@dataclass(slots=True)
class SourceFileMeta:
    filepath: Path
    dir: Path = field(init=False)
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
    fingerprint: str = field(init=False)

    def __post_init__(self):
        # Получаем данные по частям пути файла
        # Имя файла без директорий и расширения
        self.basename = self.filepath.name.split('.')[0]
        raw_extensions = self.filepath.suffixes
        # Расширение файла без точек (и .gz) 
        self.extension = next(f.lower().removeprefix('.')
                               for f in raw_extensions
                               if f in ['.fast5', '.fastq', '.fq', '.pod5'])
        self.quality_pass = True if '_pass_' in self.basename.lower() else False
        # Получаем данные по принадлежности файла
        self.batch = '_'.join(str(self.filepath.parts[-3]).split('_')[3:])
        self.sample = self.filepath.parts[-4]
        self.symlink =  self.symlink_dir / Path(":".join([self.batch,
                                                          self.sample,
                                                          self.filepath.name]))
                                                          
        # Получаем метаданные файла 
        stat = self.filepath.stat()
        self.size = stat.st_size
        self.created = datetime.fromtimestamp(stat.st_ctime)
        self.modified = datetime.fromtimestamp(stat.st_mtime)
        self.dev = stat.st_dev
        self.ino = stat.st_ino
        self.nlink = stat.st_nlink
        # Формируем отпечаток файла для отслеживания изменений
        h = hashlib.blake2s()
        for v in (
                  self.size,
                  self.created,
                  self.modified,
                  self.dev,
                  self.ino,
                  self.nlink
                 ):
            h.update(str(v).encode())
            h.update(b'|')
        self.fingerprint = h.hexdigest()


# Класс для хранения метаданных директории (необходим при инициализации:
# в случае несовпадения с данными, сохранёнными в БД - помечаем затронутые файлы/батчи/образцы как DIRTY)
@dataclass(slots=True)
class DirMeta:
    path: Path
    parent: Path
    kids: Set[Path]
    files: Set[Path] = field(default_factory=set)
    files_total: int = 0
    size_total: int = 0
    modified_ns: int = field(init=False)
    fingerprint: str = field(init=False)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib.blake2s = field(default_factory=hashlib.blake2s)

    def __post_init__(self):
        self.modified_ns = self.path.stat().st_mtime_ns
    
    def add_file2dir(self, file:SourceFileMeta, dir_kids:Set[Path]) -> None:
        if file.filepath not in self.files:
            self.files.add(file.filepath)
            self.files_total += 1
            self.size_total += file.size
            for kid in dir_kids:
                self.kids.add(kid)
            for v in (
                      self.files_total,
                      self.size_total,
                      self.modified_ns
                     ):
                self._fingerprint.update(str(v).encode())
            self.fingerprint = self._fingerprint.hexdigest()


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
    # Определяем через парсинг fast5/pod5
    experiment_type: str = field(init=False)
    flow_cell: str = field(init=False)
    sequencing_kit: str = field(init=False)
    pore: str = field(init=False)
    created: Optional[datetime] = field(default=None, init=False)
    modified: Optional[datetime] = field(default=None, init=False)
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
    # сохраняем хэш референсов для триггера обновления данных при наличии unknowns
    refs_version: str = field(default="", init=False)


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
            log_batch_info(self, "file_added", file=src.symlink.name,
                                               size=src.size,
                                               pass_=src.quality_pass)
            src_file_metadata = {}
            val_unknown = 'unknown'
            
            # Добавляем образец, к которому относится файл, к списку образцов
            self.samples.add(src.sample)
            # Обновляем дату последнего изменения батча
            self.modified = max_datetime(self.modified, src.modified)
            # Обновляем отпечаток объекта, сохраняя в нём отпечаток добавленного файла
            self._fingerprint = update_fingerprint(
                                                  main_fingerprint=self._fingerprint,
                                                  fingerprint2add=src.fingerprint
                                                  )
           
            # Проверяем, не пора ли нам добрать метадату (есть хотя бы 3 файла на чтение и метадату еще не читали)
            if all([not self.metadata_observed, len(self.files.fileset)>2]):
                log_batch_info(self, "Parsing metadata")
                #Определяем 3 кандидатов для чтения метадаты по наименьшему размеру (важно при необходимости конвертации исходника)
                candidates = sorted(list(self.files.fileset), key=lambda f: f.stat().st_size)[:3]
                # Пытаемся отобрать метадату, пробуя набрать максимум данных. В случае наличия неизвестных пробуем следующего кандидата
                for src_file in candidates:
                    # Получаем данные из файла секвенирования
                    try:
                        src_file_metadata = parse_fast5_pod5_metadata(src_file.as_posix(), val_unknown)
                        # Если метадата не все переменные определены - берём следующий образец
                        if val_unknown in src_file_metadata.values():
                            continue
                        else: break
                    # Ловим ситуацию, когда файл куда-то делся
                    except Exception:
                        log_batch_exception(self, "Trouble reading file:", file=src_file)
                
                # Больше мы файлы батча для метадаты не трогаем    
                self.metadata_observed = True
                # Проверяем, что все данные в наличии, 
                # иначе - отправляем батч на курацию
                for meta_key in ['experiment_type',
                                 'sequencing_kit',
                                 'pore',
                                 'flow_cell',
                                 'created']:
                    param_val = src_file_metadata[meta_key]
                    if  param_val == val_unknown:
                        self.needs_curation = True
                        self.unknowns.add(meta_key)
                        log_batch_warn(self, "Unknown val in metadata found", file=src_file, key=meta_key) # type: ignore
                    else:
                        if meta_key == 'created':
                            if src_file_metadata[meta_key]:
                                self.created = min_datetime(self.created, src_file_metadata[meta_key])
                                log_batch_info(self, "Set creation time:", file=src_file, time=self.created) # type: ignore
                                continue
                        log_batch_info(self, "Set variable:", key=meta_key, var=src_file_metadata[meta_key]) # type: ignore
                        setattr(self, meta_key, src_file_metadata[meta_key])


    # если раньше были unknowns, а версия обновилась — можно триггерить пересчёт на следующем цикле
    def needs_reconcile(self, current_refs_version: str) -> bool:
        return all([
                   self.needs_curation,
                   self.refs_version != current_refs_version
                  ])


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
    created: Optional[datetime] = field(default=None, init=False)
    modified: Optional[datetime] = field(default=None, init=False)
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
        self.created = min_datetime(self.created, src.created)
        self.modified = max_datetime(self.modified, src.modified)

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
                    file_meta = files_meta[file.as_posix()]
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


""""
________________________
Общие функции
________________________
"""
def upsert_dirs(
                source_d: Path,
                dirs:Dict[str, DirMeta],
                meta_file:SourceFileMeta
               ) -> Dict[str, DirMeta]:
    # Получаем список директорий файла (вплоть до source_d)
    file_dirs = lineage_map(source_d, meta_file.dir)
    for file_dir, file_dir_relatives in file_dirs.items():
        file_dir_str = file_dir.as_posix()
        if file_dir_str in dirs.keys():
            dirs[file_dir_str].add_file2dir(meta_file, set(file_dir_relatives['kids']))
        else:
            dirs[file_dir_str] = DirMeta(
                                         path=file_dir,
                                         parent=file_dir_relatives['parent'],
                                         kids=set(file_dir_relatives['kids'])
                                        )
    return dirs


def lineage_map(source_d: Path, subdir: Path) -> Dict[Path, dict]:
    """
    Строит словарь для цепочки директорий от source_d до subdir (включая обе):
      key: Path к узлу
      value: {"parent": Optional[Path], "kids": List[Path]}  # kids = следующий узел цепочки (0/1)
    """
    root = source_d.resolve()
    target = subdir.resolve()

    # Проверка вложенности
    try:
        rel = target.relative_to(root)
    except ValueError:
        raise ValueError(f"{target} не лежит внутри {root}")

    # Пошагово собираем цепочку
    parts = list(rel.parts)  # например: ["aa", "bb", "cc"]
    chain: List[Path] = [root]
    cur = root
    for part in parts:
        cur = cur / part
        chain.append(cur)

    # Формируем словарь parent/kids
    out: Dict[Path, dict] = {}
    for i, p in enumerate(chain):
        parent: Optional[Path] = chain[i-1] if i > 0 else Path()
        kids: List[Path] = [chain[i+1]] if i < len(chain) - 1 else []
        out[p] = {"parent": parent, "kids": kids}
    return out


def upsert_batch(
                 batches: Dict[str, BatchMeta],
                 src: SourceFileMeta,
                 summaries:Dict[str, Set[Path]]
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
                       final_summary=next((s for s
                                          in summaries['final_summary']
                                          if name in s.name), Path('')),
                       sequencing_summary=next((s for s
                                               in summaries['sequencing_summary']
                                               if name in s.name), Path(''))
                       )
    # добавляем файл в fast5/pod5 + pass/fail
    bm.add_file2batch(src)
    # Строка хэша состояния референсов для триггеров на случай неизвестных переменных
    bm.refs_version = refs_version 
    # Обновляем батч в списке батчей
    batches[name] = bm
    return batches


def log_batch_info(batch: BatchMeta, msg: str, **kv):
    logger.info("batch=%s %s %s", batch.name, msg, kv if kv else "")


def log_batch_warn(batch: BatchMeta, msg: str, **kv):
    logger.warning("batch=%s %s %s", batch.name, msg, kv if kv else "")


def log_batch_err(batch: BatchMeta, msg: str, **kv):
    logger.error("batch=%s %s %s", batch.name, msg, kv if kv else "")


def log_batch_exception(batch: BatchMeta, msg: str, **kv):
    logger.exception("batch=%s %s %s", batch.name, msg, kv if kv else "")


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
            log_error(error_type=e, error_message='Прервано пользователем') # pyright: ignore[reportArgumentType]
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
                metadata['pore'] = bcf_parts[1]\
                                                .removeprefix('r')\
                                                .replace('.', '')
                metadata['pore_speed'] = bcf_parts[2]  
        
        for k,v in {'flow_cell':read.run_info.flow_cell_product_code,
                    'sequencer_type':read.run_info.sequencer_position_type}.items():
            if v:
                metadata[k] = v
    return metadata
