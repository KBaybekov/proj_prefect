# -*- coding: utf-8 -*-
"""
Хранение метаданных батчей исходных файлов
"""
from . import update_file_in_meta, min_datetime, max_datetime, update_fingerprint, generate_final_fingerprint
from .source_file_meta import SourceFileMeta
from utils.logger import get_logger
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import blake2s as hashlib_blake2s
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = get_logger(name=__name__)

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
    files: Dict[Path, str] = field(default_factory=dict)
    samples: Set[str] = field(default_factory=set)
    size: int = field(default=0)
    # внутренний хэш-аккумулятор для fingerprint
    _fingerprint: hashlib_blake2s = field(default_factory=hashlib_blake2s)
    # Курирование метаданных
    status: str = field(default_factory=str)

    # Список изменений в батче по сравнению с предыдущей версией (файл:изменения)
    changes: Dict[Path, Dict[str, str|int|datetime]] = field(default_factory=dict)
    # Отпечаток предыдущей версии батча (если есть)
    previous_version: str = field(default_factory=str)
    

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any]
                 ) -> 'BatchMeta':
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
                          files={
                                 Path(file):fingerprint for file,fingerprint
                                 in doc.get("files", {}).items()
                                },
                          samples=set(doc.get("samples", [])),
                          size=doc.get("size", 0),
                          status=doc.get("status", "indexed"),
                          changes=doc.get("changes", {}),
                          previous_version=doc.get("previous_version", ""),
                         )
        # Восстанавливаем внутренний хэш-аккумулятор
        if "fingerprint" in doc:
            batch._fingerprint = hashlib_blake2s()
            batch._fingerprint = update_fingerprint(batch._fingerprint,
                                                     batch.fingerprint)

        return batch
    
    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Конвертирует объект BatchMeta в словарь.
        """
        keys2remove = []
        dict_obj = self.__dict__
        for key in dict_obj:
            if key.startswith("_"):
                keys2remove.append(key)
            if key in ["final_summary", "sequencing_summary"]:
                if dict_obj[key]:
                    dict_obj[key] = dict_obj[key].as_posix()
                else:
                    dict_obj[key] = ""
        for key in keys2remove:
            del dict_obj[key]
                    
        return dict_obj

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
        Добавляет SourceFileMeta в набор файлов.
        Ожидается, что src.symlink уже установлен.
        """
        if src.symlink not in self.files:
            self.files[src.symlink] = src.fingerprint
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
                            self._fingerprint = hashlib_blake2s()
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
        if meta_file_id in self.files:
            logger.debug(f"Удаление файла {meta_file_id} из батча {self.name}...")
            del self.files[meta_file_id]
            file_removed = True
            self.size -= file_size
            self.modified = datetime.now()
            self.changes[Path(meta_file_id)] = {'status':'deleted'}
            logger.debug(f"Файл {meta_file_id} был удалён из батча {self.name}.")
            # Обновляем отпечаток, убирая рандомные пару букв сурса
            if not self._fingerprint:
                self._fingerprint = hashlib_blake2s()
                self._fingerprint.update(self.fingerprint.encode())
            self._fingerprint = update_fingerprint(
                                                    main_fingerprint=self._fingerprint,
                                                    fingerprint2add=src['fingerprint'][1:] # type: ignore
                                                    )
            # Проверяем, остались ли ещё файлы. Если нет - помечаем батч как устаревший
            if not self.files:
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
