# -*- coding: utf-8 -*-
"""
Модуль для первичной индексации файловой системы и взаимодействия с БД
"""
from __future__ import annotations

import os
import threading
from datetime import datetime
from typing import (
                    Dict,
                    List,
                    Tuple,
                    Set,
                    Any,
                    Optional,
                    Union
                   )
from pathlib import Path

from pymongo.collection import Collection
from watchdog.observers import Observer
from watchdog.events import (
                             DirCreatedEvent,
                             DirDeletedEvent,
                             DirModifiedEvent,
                             DirMovedEvent,
                             PatternMatchingEventHandler,
                             FileCreatedEvent,
                             FileModifiedEvent,
                             FileMovedEvent,
                             FileDeletedEvent
                            )

from utils.db.db import ConfigurableMongoDAO
from utils.filesystem.metas import (
                                    BatchMeta,
                                    SampleMeta,
                                    SourceFileMeta
                                   )
from utils.refs import REFS
from utils.logger import get_logger

logger = get_logger(__name__)

class FsWatcher(PatternMatchingEventHandler):
    """
    Обработчик событий ФС
    """
    def __init__(
                 self,
                 crawler:FsCrawler,
                 patterns: List[str],
                 ignore_directories: bool,
                 case_sensitive: bool
                ):
        super().__init__(
                         patterns=patterns,
                         ignore_directories=ignore_directories,
                         case_sensitive=case_sensitive
                        )
        self.crawler:FsCrawler = crawler
        
    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        if isinstance(event, DirCreatedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        elif isinstance(event, FileCreatedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
            self.handle_file_event(event)
        return None
    
    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        if isinstance(event, DirMovedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        elif isinstance(event, FileMovedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
            self.handle_file_event(event)
        return None
    
    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        if isinstance(event, DirModifiedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        elif isinstance(event, FileModifiedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
            self.handle_file_event(event)
        return None
    
    def on_deleted(self, event: DirDeletedEvent | FileDeletedEvent) -> None:
        if isinstance(event, DirDeletedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        elif isinstance(event, FileDeletedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
            self.handle_file_event(event)
        return None
    
    def handle_file_event(self,
                          event:Union[
                                      FileCreatedEvent,
                                      FileModifiedEvent,
                                      FileMovedEvent,
                                      FileDeletedEvent
                                     ],
                                     **kwargs
                         ) -> None:

        def _is_file_moved(
                           db_meta:Dict[str, datetime | str | int | None],
                           meta_file:SourceFileMeta
                          ) -> bool:
            """
            Сравнивает пути к файлу в БД и в ФС

            :param db_meta: метаданные файла в БД
            :param meta_file: метаданные файла в ФС
            :return: True, если пути разные
            """
            if meta_file.filepath != db_meta['filepath']:
                logger.debug(f"Файл {meta_file.name} был ранее перемещён.")
                return True
            return False
        
        """Обработка события файла"""
        logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        try:
            # Действия при обнаружении события с файлами summary
            if str(event.src_path).endswith(".txt"):
                def __define_summary_type(
                                          file:Path
                                         ) -> str:
                    """
                    Определение типа файла summary по присутствию в имени файла.
                    Если тэг не найден, возвращается пустая строка
                    """
                    for summary_type in self.crawler.summaries.keys():
                        if summary_type in file.name:
                            return summary_type
                    return ""
                
                def __add_summary_2_collection(
                                               file:Path,
                                               summary_type:str
                                              ) -> None:
                    if file not in self.crawler.summaries[summary_type]:
                        self.crawler.summaries[summary_type].add(file)
                    return None
                
                def __remove_summary_from_collection(
                                                     file:Path,
                                                     summary_type:str
                                                    ) -> None:
                    if file in self.crawler.summaries[summary_type]:
                        self.crawler.summaries[summary_type].remove(file)
                    return None
                
                src_summary = Path(str(event.src_path)).resolve()
                summary_type = __define_summary_type(src_summary)
                # Если тип summary не определён, то пропускаем его - это какой-то файл, который не должен попадать в summary
                if summary_type:
                    if isinstance(event, FileCreatedEvent):
                        __add_summary_2_collection(src_summary, summary_type)
                        return None
                    # При изменении summary надо бы что-то сделать. Но на данный момент (15.10.2025) непонятно, что именно.
                    elif isinstance(event, FileModifiedEvent):
                        return None
                    elif isinstance(event, FileMovedEvent):
                        dst_summary = Path(str(event.dest_path)).resolve()
                        __remove_summary_from_collection(src_summary, summary_type)
                        __add_summary_2_collection(dst_summary, summary_type)
                        return None
                    elif isinstance(event, FileDeletedEvent):
                        __remove_summary_from_collection(src_summary, summary_type)
                        return None

            # Действия при создании файла
            if isinstance(event, FileCreatedEvent):
                def __create_meta_n_symlink(
                                           filepath:Path,
                                           symlink_dir:Path
                                           ) -> Tuple[SourceFileMeta, str]:
                    """
                    Создание метаданных файла и симлинка

                    :param filepath: путь к файлу
                    :param symlink_dir: директория, в которой будет создан симлинк
                    :return: метаданные файла и и его id
                    """
                    # Создаём метаданные файла
                    meta_file = SourceFileMeta(
                                               filepath=filepath,
                                               symlink_dir=symlink_dir
                                              )
                    meta_id = meta_file.symlink.as_posix()
                    # Создаём ссылку, если она отсутствует в папке для ссылок
                    if not meta_file.symlink.exists():
                        os.symlink(resolved_path, meta_file.symlink)
                    return meta_file, meta_id

                def __is_file_meta_the_same(
                                            db_meta:Dict[str, datetime | str | int | None],
                                            meta_file:SourceFileMeta
                                           ) -> bool:
                        """
                        Сличает отпечатки новой меты с отпечатком из БД

                        :param db_meta: метаданные файла в БД
                        :param meta_file: метаданные файла в ФС
                        :return: True, если отпечатки совпали
                        """
                        # Запрашиваем отпечаток файла
                        # Отпечатки совпали? Значит, файл не изменился с последнего индексирования, и его метаданные уже есть в БД
                        if meta_file.fingerprint == db_meta['fingerprint']:
                            logger.debug(f"Файл {meta_file.name} интактен.")
                            return True
                        return False

                db_meta:Dict[str, datetime | str | int | None]

                resolved_path = Path(str(event.src_path)).resolve()
                # Генерируем путь к директории, в которую будет создан симлинк, если он не указан в kwargs
                target_filetype_dir = kwargs.get(
                                                 "target_filetype_dir",
                                                 self.define_symlink_folder(filepath=resolved_path)
                                                )
                # Создаём базовую мету    
                meta_file, meta_id = __create_meta_n_symlink(resolved_path, target_filetype_dir)
                # Ищем файл в БД
                db_files: Dict[str, Dict[str, datetime | str | int | None]] = self.crawler.db_files
                # В случае первичной индексации...
                if db_files:
                    # Вытаскиваем метаданные из коллекции загруженных данных, попутно их удаляя из коллекции
                    db_meta = db_files.pop(meta_id, {})
                # ...и при нахождении файла в процессе мониторинга
                else:
                    db_meta = self.crawler._extract_file_info_from_db('symlink', meta_id) # type: ignore
                # Действия при обнаружении файла в БД...
                if db_meta:

                    # ...проверяем, что файл не был перемещён...
                    if _is_file_moved(
                                      db_meta=db_meta,
                                      meta_file=meta_file
                                     ):
                        # создаём событие о перемещении в случае различий
                        event = FileMovedEvent(
                                               src_path=str(db_meta['filepath']),
                                               dest_path=meta_file.filepath.as_posix(),
                                               is_synthetic=False
                                              )
                        self.handle_file_event(event, symlink=meta_file.symlink)
                    # ...и не изменено ли его содержимое
                    if __is_file_meta_the_same(
                                               db_meta=db_meta,
                                               meta_file=meta_file
                                              ):
                        return None
                    else:
                        # Создаём событие об изменении файла
                        event = FileModifiedEvent(
                                                  src_path=meta_file.filepath.as_posix(),
                                                  dest_path= '',
                                                  is_synthetic=False
                                                 )
                        self.handle_file_event(
                                               event,
                                               meta_id=meta_id,
                                               db_meta=db_meta,
                                               meta_file=meta_file,
                                               found_during_init=kwargs.get("found_during_init", False)
                                              )
                        return None

                # ...и впервые
                else:
                    meta_file.finalize()
                    self.crawler.update_batch_metadata(
                                                       meta_file=meta_file,
                                                       update_type="add"
                                                      )
                    
                    return None
            
            # действия при изменении файла
            elif isinstance(event, FileModifiedEvent):
                def __register_file_changes(
                                            db_meta:Dict[str, datetime | str | int | None],
                                            meta_file:SourceFileMeta,
                                            meta_id:str
                                           ) -> None:
                    
                    def ___extract_file_changes(
                                                props2check:List[str],
                                                db_meta:Dict[str, datetime | str | int | None],
                                                meta_file:SourceFileMeta
                                               ) -> Dict[str, List[str|int|datetime]]:
                        changed_properties = {}
                        for prop in props2check:
                            old_prop = db_meta[prop]
                            new_prop = getattr(meta_file, prop)
                            if new_prop != old_prop:
                                changed_properties[prop] = [old_prop, new_prop]
                                logger.debug(f"  {prop}: {str(old_prop)} => {str(new_prop)}")
                        if changed_properties:
                            changed_properties["fingerprint"] = [db_meta['fingerprint'], meta_file.fingerprint]
                        else:
                            logger.error(f"  Неизвестная причина изменения отпечатка файла {meta_file.name}:\n\
                                {db_meta['fingerprint']} => {meta_file.fingerprint}")
                        return changed_properties

                    def ___write_changes_to_meta_log(
                                                     meta_file:SourceFileMeta,
                                                     changed_properties:Dict[str, List[str|int|datetime]]
                                                    ) -> SourceFileMeta:
                        """
                        Записывает изменения в лог метаданных файла
                        """
                        for prop, diffs in changed_properties.items():
                            if prop != "fingerprint":
                                new_prop = diffs[1]
                                meta_file.changes[prop] = new_prop # type: ignore
                        meta_file.previous_version = changed_properties["fingerprint"][0] # type: ignore
                        return meta_file        

                    logger.debug(f"Поиск изменений в файле {meta_file.name}:")
                    meta_file.finalize()
                    # Ищем изменения в файле, касающиеся его содержимого
                    props2check = [prop for prop in self.crawler.unique_file_properties if prop != 'filepath']
                    changed_properties = ___extract_file_changes(
                                                                 props2check,
                                                                 db_meta,
                                                                 meta_file
                                                                )
                    if changed_properties:
                        meta_file = ___write_changes_to_meta_log(
                                                                 meta_file,
                                                                 changed_properties
                                                                )
                        self.crawler.file_diffs[meta_id] = changed_properties
                        self.crawler.new_indexed_files[meta_id] = meta_file
                        self.crawler.update_batch_metadata(
                                                           meta_file=meta_file,
                                                           update_type="modify",
                                                          )
                        logger.debug(f"Изменения в файле {meta_id} записаны")

                logger.debug(f"Файл {event.src_path} был изменён.")
                found_during_init = kwargs.get("found_during_init", False)
                meta_file: SourceFileMeta = kwargs.get("meta_file", None) # type: ignore
                if not meta_file:
                    filepath = Path(str(event.src_path))
                    symlink_dir = self.define_symlink_folder(filepath=filepath)
                    meta_file = SourceFileMeta(
                                               filepath=filepath,
                                               symlink_dir=symlink_dir
                                              )
                meta_id = meta_file.symlink.as_posix()

                db_meta:Dict[str, datetime | str | int | None] = kwargs.get(
                                                                            "db_meta",
                                                                            self.crawler._extract_file_info_from_db(
                                                                                                                    'symlink',
                                                                                                                    meta_id
                                                                                                                   )
                                                                           )
                if not db_meta:
                    logger.error(f"Файл {event.src_path} не был зарегистрирован в БД. Перенаправление в FileCreatedEvent")
                    event = FileCreatedEvent(
                                             src_path=event.src_path,
                                             dest_path="",
                                             is_synthetic=False
                                            )
                    self.handle_file_event(event, **kwargs)
                    return None

                # Если событие зарегистрировано в процессе первичной индексации, то дебаунс не применяется
                if found_during_init:
                    __register_file_changes(
                                            db_meta,
                                            meta_file,
                                            meta_id
                                           )
                    return None
                else:
                    timer = self.crawler.timers.get(meta_id)
                    if timer:
                        timer.cancel()
                    self.crawler.timers[meta_id] = threading.Timer(
                                                                   interval=self.crawler.file_modified_debounce,
                                                                   function=__register_file_changes,
                                                                   args=(db_meta, meta_file, meta_id)
                                                                  )
                    self.crawler.timers[meta_id].start() # type: ignore
                    return None                    
                                   
            # действия при перемещении файла
            elif isinstance(event, FileMovedEvent):
                def replace_symlink_source(symlink: Path, new_source: Path) -> None:
                    """
                    Если симлинк существует, то меняем его источник
                    """
                    if symlink.exists():
                        if symlink.is_symlink():
                            symlink.unlink()
                            symlink.symlink_to(new_source)
                            logger.debug(f"Симлинк {symlink} обновлен.")
                        else:
                            logger.error(f"{symlink} is not a symlink")

                symlink:Path = kwargs.get(
                                          "symlink",
                                          self.define_symlink_folder(
                                                                     filepath=Path(str(event.dest_path)),
                                                                    ) 
                                         )
                # Создаём новый симлинк на файл
                replace_symlink_source(
                                       symlink=symlink, 
                                       new_source=Path(event.dest_path) # type: ignore
                                      )
                # Записываем в коллекцию для обновления данных в БД
                self.crawler.new_indexed_file_moves[symlink.as_posix()] = Path(str(event.dest_path))
                logger.debug(f"Файл {event.src_path} перемещён в {event.dest_path}")
                return None
            
            # действия при удалении файла
            elif isinstance(event, FileDeletedEvent):
                db_meta = kwargs.get("db_meta",
                                     self.crawler._extract_file_info_from_db(
                                                                             meta_field="filepath",
                                                                             meta_id=str(event.src_path),
                                                                             additional_fields=[
                                                                                                'batch',
                                                                                                'sample',
                                                                                                'fingerprint',
                                                                                                'quality_pass',
                                                                                                'extension'
                                                                                               ]
                                                                            )
                                    )
                if db_meta:
                    self.crawler.new_indexed_file_deletions[str(db_meta['symlink'])] = datetime.now()
                    self.crawler.file_diffs[str(db_meta['symlink'])] = {"status": "deleted"}
                    remove_symlink(Path(str(db_meta['symlink'])))
                    # Производим удаление данных файла из меты батча и образца
                    self.crawler.update_batch_metadata(
                                                       update_type="delete",
                                                       meta_file_dict=db_meta # type: ignore
                                                      )
                else:
                    logger.error(f"Удалён незарегистрированный файл {event.src_path}")
                
        except Exception as e:
            logger.error("Ошибка обработки события : %s", e, exc_info=True)

    def define_symlink_folder(
                              self,
                              filepath: Path
                             ) -> Path:
        """
        Определяет папку для симлинка исходного файла

        :param filepath: Путь к файлу
        :return: Путь к папке для симлинка
        """
        symlink_folder = self.crawler.link_dir / "unknown_extension"
        try:
            filetype = next(
                            f[1:] for f in filepath.suffixes
                            if f"*{f}" in self.patterns  # type: ignore
                            )
            symlink_folder = self.crawler.link_dir / filetype
        except StopIteration:
            logger.error(f"Неизвестный тип файла {filepath.as_posix()}")
        finally:
            return symlink_folder


class FsCrawler():
    """
    Класс для работы с файловой системой и сохранением данных объектов в ней в БД
    """
    
    def __init__(
                 self,
                 crawler_cfg: Dict[str, Any],
                 dao: ConfigurableMongoDAO
                ):
        """
        Инициализация FsCrawler
        
        Args:
            crawler_cfg: словарь с конфигурацией
            dao: Объект DAO для взаимодействия с БД

        """
        self._cfg: Dict[str, Any]
        self.dao: ConfigurableMongoDAO
        self.source_dir: Path = Path()
        self.link_dir: Path = Path()
        self.filetypes: Tuple[str] = tuple()
        
        self.unique_file_properties = ['filepath', 'size', 'dev', 'ino', 'modified']
        # Списки саммери
        self.summaries: Dict[str, Set[Path]] = {"final_summary": set(), "sequencing_summary": set()}
        patterns2watch = [f"*.{filetype}" for filetype in self.filetypes]
        patterns2watch.extend([f"*{summary}*.txt" for summary in self.summaries.keys()])
        self.event_handler = FsWatcher(
                                       patterns=patterns2watch,
                                       ignore_directories=True,
                                       case_sensitive=False,
                                       crawler=self                                       
                                      )
        
        
        # Сюда загружаются объекты из БД, запрашиваемые при наличии новых метаданных файлов
        # В случае отсутствия батча в БД ключ имеет значение None
        self.db_files: Dict[str, Dict[str, datetime | str | int | None]] = {}
        self.loaded_files: Dict[str, SourceFileMeta] = {}
        self.loaded_batches: Dict[str, BatchMeta] = {}
        self.loaded_samples: Dict[str, SampleMeta] = {}
        
        # Словарь, содержащий различия между метаданными файлов в БД и ФС
        self.file_diffs: Dict[str, Dict[str, Any]] = {}

        # Запись в БД
        self.new_indexed_files: Dict[str, SourceFileMeta] = {}
        self.new_indexed_batches: Dict[str, BatchMeta] = {}
        self.new_indexed_samples: Dict[str, SampleMeta] = {}
        # Словарь вида "симлинк: новый путь"
        self.new_indexed_file_moves: Dict[str, Path] = {}
        # Словарь вида "симлинк: время удаления"
        self.new_indexed_file_deletions: Dict[str, datetime] = {}
        # Куда сохранять зафиксированные изменения в метаданных файлов/батчей/образцов
        self.db_collections4storing = {
                                       'new_indexed_files':'files',
                                       'new_indexed_batches':'batches',
                                       'new_indexed_samples':'samples',
                                       'new_indexed_file_moves':'files',
                                      }
        self.db_debounce:int = 10
        self.file_modified_debounce:int = 5
        self.db_update_interval:int = 30
        # таймеры и блокировки для потокобезопасности и периодической записи в БД
        self.timers: Dict[str, Optional[threading.Timer]] = {collection:None
                                                                      for collection in self.db_collections4storing.keys()}
        
        self.locks: Dict[str, threading.Lock] = {
                                                 collection:threading.Lock() 
                                                 for collection in self.db_collections4storing.keys()}
        # Состояние наблюдения
        self.observer: Optional[Observer] = None # type: ignore

    def init_crawler(
                      self
                     ) -> None:
        """
        Инициализация наблюдателя и индексация файлов
        """
        def __get_dir_obj(
                          string:str,
                          must_exist:bool = False
                         ) -> Path:
            dir_path = Path(string).resolve()
            if not dir_path.exists():
                if must_exist:
                    raise FileNotFoundError(f"Директория {dir_path.as_posix()} не существует")
                else:
                    dir_path.mkdir(parents=True)
            return dir_path

        # Инициализация директорий
        self.source_dir = __get_dir_obj(self._cfg.get("source_dir", "."), must_exist=True)
        self.link_dir = __get_dir_obj(self._cfg.get("link_dir", "./links"))
        
        # Дебаунсы
        self.db_debounce = int(self._cfg.get("db_writing_debounce", 10))
        self.file_modified_debounce = int(self._cfg.get("file_modification_debounce", 5))
        self.db_update_interval = int(self._cfg.get("db_update_interval", 60))
        self.filetypes = tuple([
                                e.strip() for e
                                in self._cfg.get("source_files_extensions", "")
                                if e.strip()
                              ])
        
        # Индексация файлов при первичной инициализации
        self.index_files()

    def _extract_file_info_from_db(
                                   self,
                                   meta_field:str,
                                   meta_id:str,
                                   additional_fields:Optional[List[str]] = None
                                  ) -> Dict[str, str|int|datetime]:
        """
        Поиск актуального (status != "deprecated"/"deleted") файла в БД по meta_id в поле meta_field
        Возвращает словарь со свойствами 'filepath', 'size', 'dev', 'ino', 'modified'
        """
        requested_properties = self.unique_file_properties
        if additional_fields:
            requested_properties.extend(additional_fields)

        return self.dao.find_one(
                                 collection='files',
                                 query={
                                        meta_field: meta_id,
                                        'status': {"$nin":["deprecated", "deleted"]}
                                       },
                                 projection={
                                             '_id':0,
                                             'symlink':1,
                                             **{prop:1 for prop in requested_properties}
                                            }
                                )
    
    def start_crawler(
                      self
                     ) -> None:
        """
        Запускает периодическую проверку изменений в файлах.
        """
        # Переходим в режим мониторинга: периодически проверяем коллекции с данными на загрузку в БД на предмет... данных
        logger.info("Запуск мониторинга...")
        self._monitor_fs()


    def index_files(self) -> None:
        """
        Первичная индексация файлов в указанной директории в потокобезопасном режиме.
        Returns:
            Словарь с индексированными данными
        """
        logger.info(f"Начата первичная индексация директории: {self.source_dir}")
        
        try:
            with threading.Lock():
                # Индексация файлов
                self.index_fs_files()
            logger.info(f"Завершена индексация файлов. Проиндексировано:\n  файлов: {len(self.new_indexed_files.keys())}\n  батчей: {len(self.new_indexed_batches.keys())}\n  образцов: {len(self.new_indexed_samples.keys())}")
            return
                
        except Exception as e:
            logger.error(f"Ошибка при индексации файлов: {str(e)}")
            raise e

    def index_fs_files(self) -> None:
        """
        Делает первичный обход папок с исходными данными и симлинками к ним,
        собирает метаданные всех файлов с указанными расширениями в виде списка SourceFileMeta.
        В случае отсутствия симлинка на исходный файл - создаёт его
        """
        # Получаем словарь вида "симлинк:отпечаток, файловый путь, размер, dev, ino, modified"
        # для файлов, проиндексированных в БД     
        self.db_files = self._get_db_files_meta()
        logger.debug(f"Получено {len(self.db_files.keys())} записей файлов из БД")
        # Получаем список файлов в директории и вложенных папках
        fs_files = get_files_by_extension_in_dir_tree(
                                                      dirs=[self.source_dir],
                                                      extensions=self.filetypes
                                                     )
        logger.debug(f"В ФС найдено {len(fs_files)} файлов")
        logger.info("Синхронизация метаданных файлов...")
        """
        db_files и fs_files формируют 3 множества:
            1. файлы, которые есть только в ФС (новые файлы)
            2. файлы, которые есть в БД и в ФС
            3. файлы, которые есть только в БД (файлы, которые были удалены из ФС)
        Соответственно, ниже отрабатываем каждое из этих множеств
        """
        # 1. файлы, которые есть в ФС
        # Формируем списки файлов для каждого типа отчётов
        for summary in self.summaries.keys():
            self.summaries[summary] = set(self.source_dir.glob(f'**/*{summary}*.txt'))
            logger.debug(f"Найдено {len(self.summaries[summary])} файлов типа {summary}")
        
        for filetype in self.filetypes:
            # Находим все файлы указанного типа в списке исходных файлов
            filetype_files = [file for file in fs_files if file.name.endswith(filetype)]
            if filetype_files:
                # Создаем целевую директорию для ссылок на файлы этого типа
                target_filetype_dir = self.link_dir / filetype if '.' not in filetype \
                                                    else self.link_dir / filetype.split('.')[0]
                target_filetype_dir.mkdir(parents=True, exist_ok=True)

                for file in filetype_files:
                    event = FileCreatedEvent(
                                             src_path=file.as_posix(),
                                             dest_path="",
                                             is_synthetic=False
                                            )
                    self.event_handler.handle_file_event(
                                                         event,
                                                         found_during_init=True,
                                                         filetype=filetype,
                                                         target_filetype_dir=target_filetype_dir
                                                        )
                    # удаляем файл из fs_files и db_files, так как он уже обработан
                    try:
                        fs_files.remove(file)

                    except ValueError:
                        logger.warning(f"Файл {file} уже был обработан")    
                    
        # Теперь проходимся по всем файлам в БД, которые не были обработаны, а значит, отсутствуют в ФС
        # Нам нужно очистить пространство от ссылок на эти файлы
        # (включая метаданные батчей и образцов, ссылки) и актуализировать метаданные в files
        if self.db_files:
            for meta_id, db_meta in self.db_files.items():
                symlink = Path(meta_id).resolve()
                # удаляем симлинк, если он есть
                remove_symlink(symlink)
                # удаляем запись файла из БД
                # нам нужно чуть больше данных о файле, чтобы отработать событие
                # поэтому запросим его заново из БД
                db_meta = self._extract_file_info_from_db(
                                                          meta_field="symlink",
                                                          meta_id=meta_id,
                                                          additional_fields=[
                                                                             'batch',
                                                                             'sample',
                                                                             'fingerprint',
                                                                             'quality_pass',
                                                                             'extension'                                                                                   
                                                                            ]
                                                         )
                self.update_batch_metadata(
                                           update_type="delete",
                                           meta_file_dict=db_meta # type: ignore
                                          )
            self.db_files = {}

        # Наконец, финализируем метаданные батчей и образцов и записываем метаданные в БД
        self._fs_changes2db(stage='init')

    def _get_db_files_meta(self) -> Dict[str, Dict[str, datetime | str | int | None]]:
        """Получение списка актуальных файлов из БД в виде словаря {симлинк файла: {свойства}}"""
        properties = {
                      k:1 for k in
                      self.unique_file_properties
                     }
        files_cursor = self.dao.find(
                                     collection='files',
                                     query={'status':{"$nin":["deprecated", "deleted"]}},
                                     projection={
                                                 '_id':0,
                                                 'symlink':1,
                                                 **properties
                                                })
        return {
                d['symlink']: {prop:d.get(prop, None) for prop in self.unique_file_properties}
                for d in files_cursor
                if 'symlink' in d
                }
    
    def _monitor_fs(self) -> None:
        def __check_new_data2fs():
            if any([getattr(self, collection) for collection in self.db_collections4storing.keys()]):
                self._fs_changes2db(stage='monitoring')
            self.timers['db_update_timer'].start() # type: ignore
        self.timers['db_update_timer'] = threading.Timer(self.db_update_interval, __check_new_data2fs)
        self.timers['db_update_timer'].start()
        
    def _get_meta_objects(
                          self,
                          obj_type:str,
                          obj_id:str
                         ) -> BatchMeta|SampleMeta:
        """
        Возвращает метаданные батча/образца.
        - Если объект был ранее изменён (FsCrawler.new_indexed_*) — возвращаем его
        - Если объект не был изменён, но выгружен из БД (FsCrawler.loaded_*) — возвращаем этот экземпляр
        - Если объект не был выгружен из БД — выгружаем, сохраняем в кэш (FsCrawler.loaded_*) и возвращаем этот экземпляр
        - Если объект до этого не существовал — создаём его, сохраняем в кэш и возвращаем

        :param obj_type: тип объекта
        :param obj_id: идентификатор объекта
        :return: объект BatchMeta|SampleMeta
        """
        meta_obj:BatchMeta|SampleMeta
        obj_config = {
                           "batch":{
                                    "loaded":self.loaded_batches,
                                    "updated":self.new_indexed_batches,
                                    "db":"batches",
                                    "class":BatchMeta
                                   },
                           "sample":{
                                     "loaded":self.loaded_samples,
                                     "updated":self.new_indexed_samples,
                                     "db":"samples",
                                     "class":SampleMeta
                                    }        
                          }
        config = obj_config[obj_type]
    
        # Проверяем обновлённые объекты
        if obj_id in config["updated"]:
            return config["updated"][obj_id]
        
        # Проверяем загруженные объекты
        if obj_id in config["loaded"]:
            return config["loaded"][obj_id]
        
        # Загружаем из БД
        doc = self.dao.find_one(
                                collection=config["db"],
                                query={
                                       "name": obj_id,
                                       "status":{"$nin": ["deprecated", "deleted"]}
                                      },
                                projection=None  # Загружаем все поля
                               )
        if doc:
            meta_obj = config["class"].from_db(doc)
            # Удаляем записи об изменениях и предыдущей версии 
            # в случае изменений эти атрибуты будут заполнены новой информацией
            meta_obj.changes = {} # type: ignore
            meta_obj.previous_version = ''
            config["loaded"][obj_id] = meta_obj
            return meta_obj
        
        # Создаём новый объект
        if obj_type == 'batch':
            meta_obj = BatchMeta(
                                 name=obj_id,
                                 final_summary=next((s for s
                                                     in self.summaries['final_summary']
                                                     if obj_id in s.name),
                                                     Path()
                                                   ),
                                 sequencing_summary=next((s for s
                                                          in self.summaries['sequencing_summary']
                                                          if obj_id in s.name),
                                                          Path()
                                                        )
                                )
        elif obj_type == 'sample':
            meta_obj = SampleMeta(name=obj_id)
        config["loaded"][obj_id] = meta_obj # type: ignore
        return meta_obj # type: ignore
        
    def update_batch_metadata(
                              self,
                              update_type:str,
                              meta_file:Optional[SourceFileMeta] = None,
                              meta_file_dict:Optional[Dict[str, str|int|datetime]] = None
                             ) -> None:
        """
        Формирует метаданные батча/образца.
        - Если объект был ранее изменён — обновляем его
        - Если объект не был изменён, но выгружен из БД — обновляем этот экземпляр
        - Если объект не был выгружен из БД — выгружаем, сохраняем в кэш и работаем с ним
        - Если объект до этого не существовал — создаём его
        В зависимости от типа обновления добавляет, изменяет или удаляет метаданные файла из меты объекта.
        Если объект - батч, то после обновления аналогичные действия проводятся и с образцами, относящимися к батчу.
        
        :param meta_file: объект SourceFileMeta
        :param update_type: тип обновления
        :param obj_type: тип объекта
        :return: словарь батчей
        """
        meta_batch: BatchMeta
        file_id:Path = Path()
        # Добавляем записи о батче и образце, к которым относится файл
        # В этой же функции батч/образец добавляются в соответствующие списки
        if meta_file:
            batch_name = meta_file.batch
            file_id = meta_file.symlink
        elif meta_file_dict:
            batch_name = str(meta_file_dict['batch'])
            file_id:Path = Path(str(meta_file_dict['symlink']))
        meta_batch = self._get_meta_objects(obj_type='batch', obj_id=batch_name) # type: ignore
        
        batch_changed = meta_batch.update_batch(
                                                file_meta=meta_file,
                                                file_meta_dict=meta_file_dict,
                                                change_type=update_type,
                                                file_diffs=self.file_diffs[file_id.as_posix()]
                                               )
        # если батч был изменён — добавляем его в список обновлённых, затем обновляем метаданные образца,
        # к которому относится изменённый файл
        if batch_changed:
            meta_batch.changes[file_id] = self.file_diffs[file_id.as_posix()]
            self.new_indexed_batches[batch_name] = meta_batch # type: ignore

            meta_sample:SampleMeta = self._get_meta_objects(obj_type='sample', obj_id=meta_file.sample) # type: ignore
            sample_changed = meta_sample.update_sample(
                                                       file_meta=meta_file, # type: ignore
                                                       batch_meta=meta_batch,
                                                       change_type=update_type,
                                                       file_diffs=self.file_diffs[file_id.as_posix()]
                                                      )
            if sample_changed:
                meta_sample.changes[meta_batch.name].update({file_id: self.file_diffs[file_id.as_posix()]})
                self.new_indexed_samples[meta_sample.name] = meta_sample
        
    def _complete_batch_metadata(self, meta_batch:BatchMeta) -> None:
        # Создаём отпечатки для каждого батча и определяем статус
        meta_batch.finalize()

    def _fs_changes2db(
                       self,
                       stage:str
                      ) -> None:
        """
        Записывает в БД данные из словаря self.new_indexed_*.
        В зависимости от stage выполняет дебаунс (ожидает, пока загружаемые коллекции 
        не будут определённое время неизменными)
        """
        def __prepare_for_transition(
                                     stage:str,
                                     collection:str,
                                     db_collection:str 
                                    ):
            data:Dict[str, Union[str, Path|SourceFileMeta|BatchMeta|SampleMeta]] = getattr(self, collection, {})
            if data:
                for meta in data.values():
                    if isinstance(meta, (BatchMeta, SampleMeta)):
                        meta.finalize()
                if stage == 'init':
                    __execute_data_transition(
                                              collection=collection,
                                              db_collection=db_collection,
                                              data=data
                                             )
                elif stage == 'monitoring':
                    if self.timers[collection]:
                    self.timers[collection].cancel() # type: ignore
                self.timers[collection] = threading.Timer(
                                                          interval=self.db_debounce,
                                                          function=__execute_data_transition,
                                                          args=[
                                                                collection,
                                                                db_collection,
                                                                data
                                                               ]
                                                         )
                self.timers[collection].start() # type: ignore
            else:
                logger.info(f"Нет данных для записи в БД: {collection}")

        def __execute_data_transition(
                                      collection:str,
                                      db_collection:str,
                                      data:Dict[str, Union[str, Path|SourceFileMeta|BatchMeta|SampleMeta]]
                                     ) -> None:
            data_lock = self.locks[collection]
            # блокируем запись в коллекцию на время записи данных в БД
            with data_lock:
                if collection == 'new_indexed_file_moves':
                    for symlink, new_filepath in data.items():
                        if new_filepath:
                            self.dao.update_one(
                                                collection=db_collection,
                                                query={
                                                       "symlink": symlink,
                                                       "status": {"$nin": ["deprecated", "deleted"]}
                                                      },
                                                doc={"filepath": new_filepath}
                                               )
                elif collection == 'new_indexed_file_deletions':
                    for symlink, time_of_deletion_registration in data.items():
                        self.dao.update_one(
                                            collection=db_collection,
                                            query={
                                                   "symlink": symlink,
                                                   "status": {"$nin": ["deprecated", "deleted"]}
                                                  },
                                            doc={
                                                 "status": "deleted",
                                                 "deleted_at": time_of_deletion_registration
                                                }
                                           )
                
                else:
                    data2upload: List[Dict[str, Any]] = [meta.__dict__ for meta in data.values()] # type: ignore
                    # Обновляем статусы устаревших метаданных
                    for meta in data2upload:
                        if meta["previous_version"]:
                            self.dao.update_one(
                                                collection=db_collection,
                                                query={"fingerprint": meta["previous_version"]},
                                                doc={"status": "deprecated"}
                                               )
                    # Добавляем новые метаданные одной загрузкой
                    self.dao.insert_many(
                                         collection=db_collection,
                                         documents=data2upload
                                        )

        for collection, db_collection in self.db_collections4storing.items():
            __prepare_for_transition(
                                     stage=stage,
                                     collection=collection,
                                     db_collection=db_collection
                                    )
            return None


def get_files_by_extension_in_dir_tree(
                                       dirs: List[Path],
                                       extensions: Tuple[str, ...],
                                       empty_ok: bool = False,
                                       exclude_dirs: Tuple[Path, ...] = (Path('.git'), Path('__pycache__')),
                                      ) -> List[Path]:
    """
    Эффективно собирает файлы по списку корней (без рекурсивной Python-функции, с os.scandir).

    :param dirs: список корневых директорий
    :param extensions: кортеж расширений (без нормализации)
    :param empty_ok: если False и файлов нет — бросаем FileNotFoundError
    :param exclude_dirs: имена каталогов, которые пропускаем
    """
    results: List[Path] = []
    ex_names = {p.name for p in exclude_dirs}

    for root in dirs:
        try:
            root = root.resolve()
        except Exception:
            root = Path(str(root)).absolute()

        stack: List[Path] = [root]
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for e in it:
                        ep = Path(e.path)
                        if e.is_dir(follow_symlinks=False):
                            if ep.name in ex_names:
                                continue
                            stack.append(ep)
                        else:
                            name = ep.name
                            if any(name.endswith(ext) for ext in extensions):
                                results.append(ep)
            except FileNotFoundError:
                continue
            except PermissionError:
                logger.warning("No permission: %s", current)
            except OSError:
                logger.exception("OS error scanning: %s", current)

    if not results and not empty_ok:
        raise FileNotFoundError("Файлы не найдены")
    return results

def remove_symlink(
                   symlink: Path
                  ) -> None:
    """
    Если симлинк существует, то удаляем его

    :param symlink: Путь к симлинку
    """
    if symlink.exists():
        if symlink.is_symlink():
            symlink.unlink()
            logger.debug(f"Симлинк {symlink} удалён.")
        else:
            logger.error(f"{symlink} - не симлинк")
    else:
        logger.error(f"Симлинк {symlink} не существует.")
    return None
