# -*- coding: utf-8 -*-
"""
Хранение метаданных образцов
"""
from . import update_file_in_meta, min_datetime, max_datetime, update_fingerprint, generate_final_fingerprint
from .source_file_meta import SourceFileMeta
from .batch_meta import BatchMeta
from utils.logger import get_logger
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import blake2s as hashlib_blake2s
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = get_logger(name=__name__)

# Класс для хранения метаданных образцов
@dataclass(slots=True)
class SampleMeta:
    """
    Класс для хранения метаданных образцов.
    """
    # Определяем при инициализации 
    name: str
    sample_id: str = field(default_factory=str)
    files: Dict[Path, str] = field(default_factory=dict)
    # Наполняем после прохода по всем файлам
    batches: Set[str] = field(default_factory=set)
    size: int = 0
    # Определяем по метаданным батчей, в которых присутствует образец
    created: Optional[datetime] = field(default=None)
    modified: Optional[datetime] = field(default=None)
    # Определяем по ходу наполнения образца файлами
    fingerprint: str = field(default_factory=str)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib_blake2s = field(default_factory=hashlib_blake2s)
        
    # данные TaskScheduler
    # словарь вида {пайплайн: {id задания: статус}}
    tasks: Dict[str, Dict[str, str]] = field(default_factory=dict)
    processing_dir: Path = field(default_factory=Path)
    result_dir: Path = field(default_factory=Path)
    
    # История изменений
    status: str = field(default_factory=str)
    # список изменений батчей и входящих в них файлов
    changes: Dict[str, Dict[Path, Dict[str, int | datetime]]] = field(default_factory=dict)
    # Отпечаток предыдущей версии батча (если есть)
    previous_version: str = field(default_factory=str)
    
    @staticmethod
    def from_db(doc: Dict[str, Any]) -> 'SampleMeta':
        """
        Создаёт объект SampleMeta из документа БД.

        :param doc: Документ из коллекции 'samples' в MongoDB.
        :return: Объект SampleMeta.
        """
        # Инициализируем основные поля SampleMeta
        sample = SampleMeta(
                            name=doc.get("name", ""),
                            sample_id=doc.get("sample_id", ""),
                            batches=set(doc.get("batches", [])),
                            files={
                                   Path(file):fingerprint for file,fingerprint
                                   in doc.get("files", {}).items()
                                  },
                            size=doc.get("size", 0),
                            created=doc.get("created"),
                            modified=doc.get("modified"),
                            fingerprint=doc.get("fingerprint", ""),
                            status=doc.get("status", "indexed"),
                            tasks=doc.get("tasks", {}),
                            changes=doc.get("changes", {}),
                            previous_version=doc.get("previous_version", ""),
                           )

        # Восстанавливаем внутренний хэш-аккумулятор
        if "fingerprint" in doc:
            sample._fingerprint = hashlib_blake2s()
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
        if file_meta.symlink not in self.files:
            self.files[file_meta.symlink] = file_meta.fingerprint
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
            file_id_pathed = Path(file_id)
            file_size = int(file_meta_dict['size']) # type: ignore
            if file_id_pathed in self.files:
                logger.debug(f"  Удаление метаданных файла {file_id}")
                del self.files[file_id_pathed]
                file_removed = True
            if file_removed:
                logger.debug(f"  Метаданные файла {file_id} удалены")
                self.size -= file_size
                self.modified = datetime.now()
                if not self._fingerprint:
                    self._fingerprint = hashlib_blake2s()
                    self._fingerprint = update_fingerprint(
                                                            self._fingerprint,
                                                            str(file_meta_dict['fingerprint'])[1:]
                                                            )
                    # Помечаем образец как неактуальный, если у него не осталось файлов И батчей на курации
                    if not self.files:
                        logger.debug(f"  Образец пуст")
                        self.status = 'deprecated'
            else:
                logger.error(f"Файл {file_id} не найден в образце {self.name}")
        return file_removed

    def finalize(
                 self,
                 processing_dir:Path,
                 result_dir:Path 
                ):
        self.fingerprint = generate_final_fingerprint(self._fingerprint)
        fingerprint_short = self.fingerprint[:6]
        self.processing_dir = (processing_dir / self.name / fingerprint_short).resolve()
        self.result_dir = (result_dir / self.name / fingerprint_short).resolve()
        self.sample_id = f"{self.name}_{fingerprint_short}"
