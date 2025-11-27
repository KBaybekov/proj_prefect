# -*- coding: utf-8 -*-
"""
Хранение метаданных батчей исходных файлов

Модуль предоставляет класс BatchMeta для сбора, хранения и управления
метаданными группы файлов (батча), включая:
- Список файлов и их отпечатков
- Связанные отчёты (summary)
- Общую информацию: размер, даты, статус
- Механизм отслеживания изменений и версионирования
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
    Класс для хранения и управления метаданными батча (группы файлов).

    Содержит информацию о составе батча, его состоянии, изменениях и связях с образцами.
    Поддерживает версионирование через механизм отпечатков (fingerprint) и отслеживание изменений.
    """
    # Определяем при инициализации 
    name: str
    """
    Уникальное имя батча (обычно соответствует имени папки с данными).
    """

    final_summary: Path
    """
    Путь к финальному отчёту (например, final_summary.txt) для этого батча.
    """

    sequencing_summary: Path
    """
    Путь к отчёту секвенирования (например, sequencing_summary.txt) для этого батча.
    """

    created: Optional[datetime] = field(default=None)
    """
    Дата и время создания батча (на основе самого старого файла в нём).
    Устанавливается при добавлении первого файла.
    """

    modified: Optional[datetime] = field(default=None)
    """
    Дата и время последнего изменения батча (на основе самого нового файла).
    Обновляется при добавлении или модификации файлов.
    """

    fingerprint: str = field(default_factory=str)
    """
    Финальный отпечаток (хэш) батча, вычисляемый из отпечатков всех файлов.
    Генерируется при вызове finalize().
    """

    files: Dict[Path, str] = field(default_factory=dict)
    """
    Словарь файлов в батче: ключ — симлинк на файл, значение — его отпечаток.
    Позволяет отслеживать состав и целостность батча.
    """

    samples: Set[str] = field(default_factory=set)
    """
    Множество имён образцов, файлы которых входят в этот батч.
    Заполняется автоматически при добавлении файлов.
    """

    size: int = field(default=0)
    """
    Общий размер всех файлов в батче в байтах.
    Обновляется при добавлении/удалении файлов.
    """

    _fingerprint: hashlib_blake2s = field(default_factory=hashlib_blake2s)
    """
    Внутренний аккумулятор хэшей для построения итогового fingerprint.
    Работает как потоковый хэш — в него последовательно добавляются отпечатки файлов.
    """

    status: str = field(default_factory=str)
    """
    Текущий статус батча. Возможные значения:
    - 'indexed' — проиндексирован и актуален
    - 'deprecated' — устарел (например, все файлы удалены)
    - 'deleted' — помечен на удаление
    """

    changes: Dict[Path, Dict[str, str|int|datetime]] = field(default_factory=dict)
    """
    Словарь изменений в батче по сравнению с предыдущей версией.
    Ключ — путь к файлу, значение — словарь с изменёнными свойствами.
    Используется для отслеживания истории изменений.
    """

    previous_version: str = field(default_factory=str)
    """
    Отпечаток предыдущей версии батча.
    Позволяет установить связи между версиями и пометить старую версию как устаревшую.
    """

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any]
                 ) -> 'BatchMeta':
        """
        Создаёт экземпляр BatchMeta из документа MongoDB.

        :param doc: Словарь с данными из коллекции 'batches'.
        :type doc: Dict[str, Any]
        :return: Инициализированный объект BatchMeta.
        :rtype: BatchMeta
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
        Преобразует объект BatchMeta в словарь для сохранения в MongoDB.

        Убирает служебные поля (начинающиеся с подчёркивания) и преобразует Path в строку.

        :return: Словарь с сериализованными данными батча.
        :rtype: Dict[str, Any]
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
        Обновляет метаданные батча в ответ на изменение файла.

        Выполняет одну из операций: добавление, удаление или модификация файла.
        Возвращает True, если состояние батча изменилось.

        :param change_type: Тип изменения: 'add', 'delete', 'modify'.
        :type change_type: str
        :param file_meta: Объект метаданных файла (используется при прямом доступе).
        :type file_meta: Optional[SourceFileMeta]
        :param file_meta_dict: Словарь с метаданными файла (используется при косвенном доступе).
        :type file_meta_dict: Optional[Dict[str, Union[str, int, datetime]]]
        :param file_diffs: Словарь с описанием изменений файла.
        :type file_diffs: dict
        :return: True, если батч был изменён; False — если изменений не произошло.
        :rtype: bool
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

    def add_file2batch(
                       self,
                       src: SourceFileMeta
                      ) -> bool:
        """
        Добавляет файл в батч.

        :param src: Объект метаданных файла.
        :type src: SourceFileMeta
        :return: True, если файл был успешно добавлен; False — если уже существует.
        :rtype: bool
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

    def edit_file_meta(
                       self,
                       edit_dict: Dict[Path, Dict[str, List[Any]]]
                      ) -> bool:
        """
        Обновляет метаданные файла в батче (размер, дату и т.д.).

        :param edit_dict: Словарь изменений, где ключ — путь к файлу, значение — словарь изменений.
        :type edit_dict: Dict[Path, Dict[str, List[Any]]]
        :return: True, если батч был изменён.
        :rtype: bool
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

    def remove_file_from_batch(
                               self,
                               src: Dict[str, str|int|datetime]
                              ) -> bool:
        """
        Удаляет файл из батча.

        :param src: Словарь с метаданными удаляемого файла.
        :type src: Dict[str, Union[str, int, datetime]]
        :return: True, если файл был удалён; False — если не найден.
        :rtype: bool
        """

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

    def finalize(
                 self
                ) -> None:
        """
        Финализирует метаданные батча: вычисляет итоговый отпечаток.

        Должен вызываться перед сохранением в БД.
        Также устанавливает статус 'indexed', если он не был задан.
        """
        self.fingerprint = generate_final_fingerprint(self._fingerprint)
        # помечаем батч как проиндексированный
        if not self.status:
            self.status = 'indexed'
