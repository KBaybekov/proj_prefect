# -*- coding: utf-8 -*-
"""
Хранение метаданных образцов

Модуль предоставляет класс SampleMeta для сбора, хранения и управления
метаданными образцов (семплов), включая:
- Связь с файлами и батчами
- Агрегированную информацию о состоянии и составе
- Интеграцию с системой заданий (TaskScheduler)
- Механизмы версионирования и отслеживания изменений
"""
from . import update_file_in_meta, min_datetime, max_datetime, update_fingerprint, generate_final_fingerprint
from .source_file_meta import SourceFileMeta
from .batch_meta import BatchMeta
from modules.logger import get_logger
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
    Класс для хранения и управления метаданными образца (семпла).

    Объединяет данные из нескольких батчей и файлов, агрегирует их
    и предоставляет единый интерфейс для доступа к результатам анализа.
    Служит основой для запуска пайплайнов и управления жизненным циклом образца.
    """
    # Определяем при инициализации 
    name: str
    """
    Имя образца (например, 'Sample_001').
    Может использоваться как база для формирования sample_id.
    """

    sample_id: str = field(default_factory=str)
    """
    Уникальный идентификатор образца, формируемый как {name}_{fingerprint[:6]}.
    Используется как ключ в БД и для построения путей к данным.
    """

    files: Dict[Path, str] = field(default_factory=dict)
    """
    Словарь файлов, принадлежащих образцу: ключ — симлинк, значение — отпечаток.
    Включает все файлы из всех батчей, связанных с этим образцом.
    """

    batches: Set[str] = field(default_factory=set)
    """
    Множество имён батчей, содержащих файлы этого образца.
    Обновляется при добавлении/удалении батчей.
    """

    size: int = 0
    """
    Общий размер всех файлов образца в байтах.
    Обновляется при добавлении или удалении файлов.
    """

    created: Optional[datetime] = field(default=None)
    """
    Дата создания образца (на основе самого старого файла или батча).
    """

    modified: Optional[datetime] = field(default=None)
    """
    Дата последнего изменения образца (на основе самого нового файла или батча).
    Обновляется при любых изменениях.
    """

    fingerprint: str = field(default_factory=str)
    """
    Итоговый отпечаток образца, вычисляемый из отпечатков всех файлов.
    Генерируется при вызове finalize().
    """

    _fingerprint: hashlib_blake2s = field(default_factory=hashlib_blake2s)
    """
    Внутренний аккумулятор хэшей для построения итогового fingerprint.
    Позволяет инкрементально добавлять отпечатки файлов.
    """
        
    tasks: Dict[str, Dict[str, str]] = field(default_factory=dict)
    """
    Словарь заданий, связанных с образцом.
    Формат: {"pipeline_name": {"task_id": "status"}}.
    Используется для отслеживания прогресса анализа.
    """

    processing_dir: Path = field(default_factory=Path)
    """
    Путь к директории обработки образца.
    Формируется как: {base_processing_dir}/{name}/{fingerprint[:6]}.
    """

    result_dir: Path = field(default_factory=Path)
    """
    Путь к директории результатов анализа образца.
    Формируется как: {base_result_dir}/{name}/{fingerprint[:6]}.
    """

    status: str = field(default_factory=str)
    """
    Текущий статус образца. Возможные значения:
    - 'indexed' — проиндексирован и актуален
    - 'deprecated' — устарел (например, все файлы удалены)
    - 'deleted' — помечен на удаление
    """

    changes: Dict[str, Dict[Path, Dict[str, int | datetime]]] = field(default_factory=dict)
    """
    История изменений по батчам: {batch_name: {file_path: {property: value}}}.
    Используется для отслеживания разницы между версиями.
    """

    previous_version: str = field(default_factory=str)
    """
    Отпечаток предыдущей версии образца.
    Позволяет установить связи между версиями и пометить старую как устаревшую.
    """

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any]
                 ) -> 'SampleMeta':
        """
        Создаёт экземпляр SampleMeta из документа MongoDB.

        :param doc: Словарь с данными из коллекции 'samples'.
        :type doc: Dict[str, Any]
        :return: Инициализированный объект SampleMeta.
        :rtype: SampleMeta
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
                            processing_dir=Path(doc.get("processing_dir", "")),
                            result_dir=Path(doc.get("result_dir", "")),
                            changes=doc.get("changes", {}),
                            previous_version=doc.get("previous_version", ""),
                           )

        # Восстанавливаем внутренний хэш-аккумулятор
        if "fingerprint" in doc:
            sample._fingerprint = hashlib_blake2s()
            sample._fingerprint = update_fingerprint(sample._fingerprint,
                                                     sample.fingerprint)
        
        return sample
    
    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Преобразует объект SampleMeta в словарь для сохранения в MongoDB.

        Выполняет сериализацию сложных типов: Path → str, set → list, убирает служебные поля.

        :return: Сериализованный словарь с метаданными образца.
        :rtype: Dict[str, Any]
        """
        dict_obj = self.__dict__
        keys2remove = []
        for key in dict_obj:
            if key.startswith("_"):
                keys2remove.append(key)
            
            if key in ["processing_dir", "result_dir"]:
                if dict_obj[key]:
                    dict_obj[key] = dict_obj[key].as_posix()
                else:
                    dict_obj[key] = ""
            
            if key == "files":
                if dict_obj[key]:
                    dict_obj[key] = {file.as_posix():fingerprint for file,fingerprint in dict_obj[key].items()}
                else:
                    dict_obj[key] = {}
            
            if key == "batches":
                if dict_obj[key]:
                    dict_obj[key] = list(dict_obj[key])
                else:
                    dict_obj[key] = []
        
        for key in keys2remove:
            del dict_obj[key]
                    
        return dict_obj

    def update_sample(
                      self,
                      change_type:str,
                      batch_meta: BatchMeta,
                      file_meta: Optional[SourceFileMeta]=None,
                      file_meta_dict:Optional[Dict[str, str|int|datetime]]=None,
                      file_diffs:dict={}
                     ) -> bool:
        """
        Обновляет метаданные образца в ответ на изменение файла.

        Выполняет добавление, удаление или модификацию файла.
        Возвращает True, если состояние образца изменилось.

        :param change_type: Тип изменения: 'add', 'delete', 'modify'.
        :type change_type: str
        :param batch_meta: Ссылка на метаданные батча, к которому относится файл.
        :type batch_meta: BatchMeta
        :param file_meta: Объект метаданных файла (если доступен).
        :type file_meta: Optional[SourceFileMeta]
        :param file_meta_dict: Словарь с метаданными файла (если file_meta недоступен).
        :type file_meta_dict: Optional[Dict[str, Union[str, int, datetime]]]
        :param file_diffs: Словарь с описанием изменений файла.
        :type file_diffs: dict
        :return: True, если образец был изменён; False — в противном случае.
        :rtype: bool
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

    def add_batch2sample(
                         self,
                         batch_meta: BatchMeta,
                         file_meta: SourceFileMeta
                        ) -> bool:
        """
        Добавляет батч в список батчей образца и обновляет агрегированные метаданные.

        :param batch_meta: Метаданные батча.
        :type batch_meta: BatchMeta
        :param file_meta: Метаданные файла, добавляемого из батча.
        :type file_meta: SourceFileMeta
        :return: True, если образец был изменён.
        :rtype: bool
        """
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
        Добавляет файл в образец.

        :param file_meta: Метаданные файла.
        :type file_meta: SourceFileMeta
        :return: True, если файл был успешно добавлен; False — если уже существует.
        :rtype: bool
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

    def edit_file_meta(
                       self,
                       edit_dict: Dict[Path, Dict[str, List[Any]]]
                      ) -> bool:
        """
        Обновляет метаданные файлов в образце (размер, дату и т.д.).

        :param edit_dict: Словарь изменений.
        :type edit_dict: Dict[Path, Dict[str, List[Any]]]
        :return: True, если образец был изменён.
        :rtype: bool
        """
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
        """
        Удаляет файл из образца.

        :param file_meta_dict: Словарь с метаданными удаляемого файла.
        :type file_meta_dict: Dict[str, Union[str, int, datetime, bool]]
        :return: True, если файл был удалён; False — если не найден.
        :rtype: bool
        """
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
        """
        Финализирует метаданные образца: вычисляет отпечаток и формирует идентификаторы.

        :param processing_dir: Базовая директория для обработки.
        :type processing_dir: Path
        :param result_dir: Базовая директория для результатов.
        :type result_dir: Path
        """
        self.fingerprint = generate_final_fingerprint(self._fingerprint)
        fingerprint_short = self.fingerprint[:6]
        self.processing_dir = (processing_dir / self.name / fingerprint_short).resolve()
        self.result_dir = (result_dir / self.name / fingerprint_short).resolve()
        self.sample_id = f"{self.name}_{fingerprint_short}"
