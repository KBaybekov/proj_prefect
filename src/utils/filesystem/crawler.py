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
                             PatternMatchingEventHandler,
                             FileCreatedEvent,
                             FileModifiedEvent,
                             FileMovedEvent,
                             FileDeletedEvent
                            )

from utils.db import ConfigurableMongoDAO

from utils.filesystem.metas import (
                            BatchMeta,
                            SampleMeta,
                            SourceFileMeta,
                            FileSet,
                            FileSubset,
                            FileGroup
                           )
# from utils.filesystem import ()
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
                 case_sensitive: bool,
                 dao:ConfigurableMongoDAO,
                 main_symlink_dir:Path,
                 debounce: int
                ):
        super().__init__(
                         patterns=patterns,
                         ignore_directories=ignore_directories,
                         case_sensitive=case_sensitive
                        )
        self.crawler:FsCrawler = crawler
        self.debounce_time:int = debounce
        self.dao:ConfigurableMongoDAO = dao
        self.main_symlink_dir:Path = main_symlink_dir
        self.unique_file_properties = crawler.unique_file_properties

    '''def on_created(self, event:FileCreatedEvent, *, meta_prepared:bool=False) -> None: # type: ignore
        if meta_prepared:
            # Данный блок исполняется при первичной индексации файлов
            # Действия по изменению меты батча и образца выполняются в index_files()
            logger.debug(f"Файл {event.src_path} создан, выполнена первичная индексация")
        else:
            # Обработка новых файлов, созданных в реальном времени            
            resolved_path = Path(str(event.src_path)).resolve()
            try:
                # Ищем расширения, указанные в конфиге
                file_extension = next(
                                    f[1:] for f in resolved_path.suffixes
                                    if f"*{f}" in self.patterns # type: ignore
                                    )
            except StopIteration:
                logger.error(f"Неизвестный тип файла {event.src_path}")
                return None
            target_filetype_dir = self.main_symlink_dir / file_extension

            def process_file():
                try:
                    # Проверяем, что файл всё ещё существует
                    if not resolved_path.exists():
                        logger.warning(f"Файл {resolved_path} исчез до завершения обработки")
                        return

                    # Создаём метаданные файла
                    meta = SourceFileMeta(
                        filepath=resolved_path,
                        symlink_dir=target_filetype_dir
                    )

                    # Создаём симлинк, если его ещё нет
                    if not meta.symlink.exists():
                        os.symlink(meta.filepath, meta.symlink)

                    # Вызываем обработку с meta_prepared=True
                    self.on_created(event, meta_prepared=True)
                except Exception as e:
                    logger.error(f"Ошибка при обработке созданного файла {resolved_path}: {e}")

            # Запускаем debounce
            timer = threading.Timer(self.debounce_time, process_file)
            timer.start()
        return None

    def on_modified(self, event:FileModifiedEvent, *, old_file_fingerprint:Optional[str]) -> None: # type: ignore
        self.dao.update_one(
                            collection="files",
                            key={"fingerprint":old_file_fingerprint}, # type: ignore
                            doc={"status":"deprecated"}
                           )
        logger.debug(f"Файл {event.src_path} изменён, статус deprecated установлен для {old_file_fingerprint}")
        return None
    
    def on_moved(self, event:FileMovedEvent, *, symlink:Optional[Path]) -> None: # type: ignore
        """
        Обработка события перемещения файла        
        """
        # Обновляем в базе данных информацию о файле
        self.dao.update_one(
                        collection="files",
                        key={"filepath":event.src_path},
                        doc={"filepath":event.dest_path}
                       )
        # Создаём новый симлинк на файл
        replace_symlink_source(
                                symlink=symlink, # type: ignore
                                new_source=Path(event.dest_path) # type: ignore
                                )
        logger.debug(f"Файл {event.src_path} перемещён в {event.dest_path}")
        return None
    
    def on_deleted(self, event) -> None:
        return None'''
    
    def handle_file_event(self,
                          event:Union[
                                      FileCreatedEvent,
                                      FileModifiedEvent,
                                      FileMovedEvent,
                                      FileDeletedEvent
                                     ],
                                     **kwargs
                         ) -> None:
        """Обработка события файла"""
        logger.debug(f"Событие: {type(event)}. Путь: {event.src_path}")
        try:
            # Действия при создании файла
            if isinstance(event, FileCreatedEvent):
                def _create_meta_n_symlink(
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
                                            filepath=resolved_path,
                                            symlink_dir=target_filetype_dir
                                            )
                    meta_id = meta_file.symlink.as_posix()
                    # Создаём ссылку, если она отсутствует в папке для ссылок
                    if not meta_file.symlink.exists():
                        os.symlink(resolved_path, meta_file.symlink)
                    return meta_file, meta_id
                
                def _are_metas_similar(
                                       db_meta:Dict[str, datetime | str | int | None],
                                       meta_file:SourceFileMeta
                                      ) -> Tuple[bool, SourceFileMeta]:
                    """
                    Сравнивает метаданные файла с метаданными в БД. Создаёт новый симлинк, если файл был перемещён.
                    Финализирует метаданные файла, если они отличаются от метаданных в БД

                    :param db_meta: метаданные файла в БД
                    :param meta_file: метаданные файла в ФС
                    :return: True, если метаданные совпали, иначе False
                    """
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

                    def _is_file_meta_the_same(
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
                    # ...и не изменено его содержимое
                    if _is_file_meta_the_same(
                                              db_meta=db_meta,
                                              meta_file=meta_file
                                             ):
                        return True, meta_file
                    else:
                        meta_file.finalize()
                        return False, meta_file

                def _collect_file_changes(
                                          db_meta:Dict[str, datetime | str | int | None],
                                          meta_file:SourceFileMeta
                                         ) -> Dict[str, List[int | datetime]]:
                    logger.debug(f"Поиск изменений в файле {meta_file.name}:")
                    changed_properties = {}
                    for prop in self.unique_file_properties:
                        old_prop = db_meta[prop]
                        new_prop = getattr(meta_file, prop)
                        if new_prop != old_prop:
                            changed_properties[prop] = [old_prop, new_prop]
                            logger.debug(f"  {prop}: {str(old_prop)} => {str(new_prop)}")
                    if not changed_properties:
                        logger.error(f"Неизвестная причина изменения отпечатка файла {meta_file.name}:\n\
                            {db_meta['fingerprint']} => {meta_file.fingerprint}")
                    changed_properties["fingerprint"] = [db_meta['fingerprint'], meta_file.fingerprint]
                    return changed_properties
                
                found_during_init = kwargs.get("found_during_init", False)
                resolved_path = Path(str(event.src_path)).resolve()

                # Данный блок исполняется при первичной индексации файлов
                if found_during_init:
                    logger.debug(f"Файл обнаружен: {event.src_path}")
                    filetype = kwargs.get("filetype", "")
                    
                    # Генерируем путь к директории, в которую будет создан симлинк, если он не указан в kwargs
                    target_filetype_dir = kwargs.get("target_filetype_dir", Path())
                    meta_file, meta_id = _create_meta_n_symlink(resolved_path, target_filetype_dir)
                    # Ищем данные о файле в БД, если нам не передан словарь с этими данными
                    db_files: Dict[str, Dict[str, datetime | str | int | None]] = kwargs.get("db_files", {})
                    db_meta:Dict[str, datetime | str | int | None] = db_files.get(meta_id, {})
                    # действия при наличии метаданных файла в БД (поиск - по симлинку)
                    if db_meta:
                        # Сличаем метаданные файла с метаданными в БД:
                        meta_is_same, meta_file = _are_metas_similar(
                                                                     db_meta=db_meta,
                                                                     meta_file=meta_file
                                                                    )
                        if meta_is_same:
                            return None
                        else:
                            changed_properties = _collect_file_changes(
                                                                       db_meta=db_meta,
                                                                       meta_file=meta_file
                                                                      )
                            if changed_properties:
                                # Создаём событие об изменении файла
                                event = FileModifiedEvent(
                                                          src_path=meta_file.filepath.as_posix(),
                                                          dest_path= '',
                                                          is_synthetic=False
                                                         )
                                self.handle_file_event(event,
                                                       changed_properties=changed_properties,
                                                       found_during_init=found_during_init,
                                                       meta_id=meta_id,
                                                       db_meta=db_meta,
                                                       meta_file=meta_file
                                                      )
                                return None
                            else:
                                return None
                           
                # Действия при мониторинге ФС
                else:
                    try:
                        filetype = next(
                                        f[1:] for f in resolved_path.suffixes
                                        if f"*{f}" in self.patterns # type: ignore
                                       )
                    except StopIteration:
                        logger.error(f"Неизвестный тип файла {event.src_path}")
                        return None
                    target_filetype_dir = self.main_symlink_dir / filetype
                    meta_file, meta_id = _create_meta_n_symlink(resolved_path, target_filetype_dir)
                    db_meta = self.dao.find_one(
                                                collection='files',
                                                query={
                                                       'symlink':meta_id,
                                                       'status':{"$nin":["deprecated", "deleted"]}
                                                      },
                                                projection={
                                                            '_id':0,
                                                            'symlink':1,
                                                            **{prop:1 for prop in self.unique_file_properties}
                                                           }
                                               ) # type: ignore
                        









                logger.debug(f"Создан файл: {event.src_path}")
            
            # действия при изменении файла
            elif isinstance(event, FileModifiedEvent):
                def _apply_modifications(
                                         db_meta:Dict[str, datetime | str | int | None],
                                         meta_file:SourceFileMeta,
                                         meta_id:str,
                                         changed_properties:Dict[str, List[int | datetime]],
                                         found_during_init:bool
                                        ) -> None:
                    if found_during_init:
                        # Записываем изменения в Crawler

                    else:
                        pass

                def _write_changes_to_meta_log(
                                               meta_file:SourceFileMeta,
                                               changed_properties:Dict[str, List[int | datetime]],
                                               old_fingerprint:str = ""
                                              ) -> SourceFileMeta:
                    """
                    Записывает изменения в лог метаданных файла
                    """
                    for prop, diffs in changed_properties.items():
                        new_prop = diffs[1]
                        meta_file.changes[prop] = new_prop
                    meta_file.previous_version = old_fingerprint
                    return meta_file

                logger.debug(f"Файл {event.src_path} был изменён.")
                changed_properties:Dict[str, List[int | datetime]] = kwargs.get("changed_properties", {})
                meta_file: SourceFileMeta = kwargs.get("meta_file", SourceFileMeta)
                meta_id:str = kwargs.get("meta_id", "")
                db_meta:Dict[str, datetime | str | int | None] = kwargs.get("db_meta", {})

                updated_meta_file = _write_changes_to_meta_log(
                                                               meta_file=meta_file,
                                                               changed_properties=changed_properties
                                                              ) 
                # индикатор процесса первичной индексации, при котором используется групповая запись изменений в БД
                found_during_init = kwargs.get("found_during_init", False)
                self.crawler.update_batch_metadata(
                                                   meta_file=updated_meta_file,
                                                   update_during_init=found_during_init
                                                   )
                    

                
            # действия при перемещении файла
            elif isinstance(event, FileMovedEvent):
                symlink:Path = kwargs.get("symlink", Path())
                # Создаём новый симлинк на файл
                replace_symlink_source(
                                        symlink=symlink, 
                                        new_source=Path(event.dest_path) # type: ignore
                                        )
                # Обновляем в базе данных информацию о файле
                self.dao.update_one(
                                    collection="files",
                                    key={"filepath":event.src_path},
                                    doc={"filepath":event.dest_path}
                                   )
                logger.debug(f"Файл {event.src_path} перемещён в {event.dest_path}")
                logger.debug(f"Симлинк {symlink} обновлен.")
                return None
            
            # действия при удалении файла
            elif isinstance(event, FileDeletedEvent):
                self.crawler._process_deleted_files([paths[0]], {})
                
        except Exception as e:
            logger.error("Ошибка обработки события : %s", e, exc_info=True)
    

class FsCrawler():
    """
    Класс для работы с файловой системой и сохранением данных объектов в ней в БД
    """
    
    def __init__(
                 self,
                 source_dir: Path,
                 link_dir:Path, 
                 dao: ConfigurableMongoDAO,
                 filetypes: tuple,
                 debounce: int
                ):
        """
        Инициализация FsCrawler
        
        Args:
            source_dir: Корневая директория для сканирования
            dao: Объект DAO для взаимодействия с БД
            filetypes: Поддерживаемые типы файлов
            recursive: Рекурсивное сканирование поддиректорий
        """
        self.source_dir = Path(source_dir).resolve()
        self.link_dir = Path(link_dir).resolve()
        self.dao = dao
        self.filetypes: Tuple[str] = filetypes
        self.unique_file_properties = ['filepath', 'size', 'dev', 'ino', 'modified']
        self.event_handler = FsWatcher(
                                       patterns=[f"*.{filetype}"
                                                 for filetype in self.filetypes],
                                       ignore_directories=True,
                                       case_sensitive=False,
                                       crawler=self,
                                       dao=dao,
                                       main_symlink_dir=self.link_dir,
                                       debounce=debounce
                                      )
        self._lock = threading.Lock()
        
        # Состояние индексации
        self.summaries: Dict[str, Set[Path]] = {"final_summary": set(), "sequencing_summary": set()}
        # Сюда загружаются объекты из БД, запрашиваемые при наличии новых метаданных файлов
        # В случае отсутствия батча в БД ключ имеет значение None
        self.loaded_files: Dict[str, Optional[SourceFileMeta]] = {}
        self.loaded_batches: Dict[str, Optional[BatchMeta]] = {}
        self.loaded_samples: Dict[str, Optional[SampleMeta]] = {}
        # Словари, содержащие метаданные файлов, проиндексированные впервые
        self.new_indexed_files: Dict[str, SourceFileMeta] = {}
        self.new_indexed_batches: Dict[str, BatchMeta] = {}
        self.new_indexed_samples: Dict[str, SampleMeta] = {}
        # Словарь, содержащий объекты для курации
        self.curations: Dict[str, BatchMeta] = {}
        # Словарь, содержащий различия между метаданными файлов в БД и ФС
        self.file_diffs: Dict[str, Dict[str, Any]] = {}
        
        # Состояние наблюдения
        self.observer: Optional[Observer] = None # type: ignore
        
        # Индексация файлов при первичной инициализации
        self.index_files()


    def index_files(self) -> None:
        """
        Первичная индексация файлов в указанной директории в потокобезопасном режиме.
        Returns:
            Словарь с индексированными данными
        """
        logger.info(f"Начата первичная индексация директории: {self.source_dir}")
        
        try:
            with self._lock:
                # Индексация файлов
                self.index_fs_files()
                logger.info(f"Завершена индексация файлов. Проиндексировано:\n  файлов: {len(self.new_indexed_files.keys())}\n  батчей: {len(self.new_indexed_batches.keys())}\n  образцов: {len(self.new_indexed_samples.keys())}")
                logger.info(f"Объектов для курации: {self.curations.keys()}")
                return 
                
        except Exception as e:
            logger.error(f"Ошибка при индексации файлов: {str(e)}")
            raise


    def index_fs_files(self) -> None:
        """
        Делает первичный обход папок с исходными данными и симлинками к ним,
        собирает метаданные всех файлов с указанными расширениями в виде списка SourceFileMeta.
        В случае отсутствия симлинка на исходный файл - создаёт его
        """
        # Получаем словарь вида "симлинк:отпечаток, файловый путь, размер, dev, ino, modified"
        # для файлов, проиндексированных в БД     
        db_files = self._get_db_files_meta()
        logger.debug(f"Получено {len(db_files.keys())} записей файлов из БД")
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
                
        logger.info("Метаданные файлов синхронизированы. Актуализация метаданных батчей и образцов...")
        # Собрав данные об исходных файлах и батчах, заполняем информацию по образцам
        for meta_batch in self.new_indexed_batches.values():
            self.complete_batch_metadata(meta_batch)
            # дополняем метадату образцов
            self.form_sample_metadata(meta_batch)        
        
        for meta_sample in self.new_indexed_samples.values():
            #if meta_sample.status == 'indexed':
            meta_sample.finalize()


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


    def _form_file_metadata(self,
                           file:Path,
                           target_filetype_dir:Path,
                           db_files:Dict,
                          ) -> SourceFileMeta:
        """
        Формирует метаданные файла:
        - если файл уже есть в БД — проверяет, не изменился ли он
        - если файл не был проиндексирован — создает его мету
        """

        # Получаем  базовые метаданные файла - имя, путь, размер, отпечаток, статы. Генерируем симлинк и отпечаток
        meta_file = SourceFileMeta(
                                   filepath=file,
                                   symlink_dir=target_filetype_dir
                                  )
        meta_id = meta_file.symlink.as_posix()
        # Создаём ссылку, если она отсутствует в папке для ссылок
        if not meta_file.symlink.exists():
            os.symlink(file, meta_file.symlink)
        # Проверяем, нет ли файла с идентичным именем в БД
        db_meta = db_files.get(meta_id)
        if db_meta:
            # Проверяем, совпадают ли файловые пути. Если нет - передаём в вотчдог FileMovedEvent
            if meta_file.filepath != db_meta['filepath']:
                event = FileMovedEvent(
                                       src_path=db_meta['filepath'],
                                       dest_path=meta_file.filepath.as_posix(),
                                       is_synthetic=False
                                      )
                self.event_handler.on_moved(event, symlink=meta_file.symlink)
            
            # Запрашиваем отпечаток файла
            # Отпечатки совпали? Значит, файл не изменился с последнего индексирования, и его метаданные уже есть в БД
            if meta_file.fingerprint == db_meta['fingerprint']:
                logger.debug(f"Файл {meta_file.name} уже индексирован в БД")
                return meta_file
            # Если отпечаток отличается, значит, файл был изменён
            else:
                # Создаём словарь с различиями между версиями файла
                changed_properties = {"fingerprint":[db_meta['fingerprint'], meta_file.fingerprint]}
                for prop in self.unique_file_properties:
                    old_prop = db_meta[prop]
                    new_prop = getattr(meta_file, prop)
                    if new_prop != old_prop:
                        changed_properties[prop] = [old_prop, new_prop]
                if changed_properties:
                    logger.debug(f"Найдены новые свойства для {meta_file.name}:\n{changed_properties}")
                    # Помечаем запись файла в БД как "deprecated"
                    event = FileModifiedEvent(
                                              src_path=meta_file.filepath.as_posix(),
                                              dest_path= '',
                                              is_synthetic=False
                                             )
                    self.event_handler.on_modified(
                                                   event,
                                                   old_file_fingerprint=db_meta['fingerprint']
                                                  )
                    # Делаем пометку в новой мете об изменениях и отпечатке старой меты
                    for prop, diffs in changed_properties.items():
                        # отпечаток в список изменений не вписываем - он отмечается в previous_version
                        if prop == 'fingerprint':
                            continue
                        new_prop = diffs[1]
                        meta_file.changes[prop] = new_prop
                    meta_file.previous_version = db_meta['fingerprint']
                    # Записываем различия между версиями файла в собственный словарь FsCrawler
                    self.file_diffs[meta_id] = changed_properties
                else:
                    logger.error(f"Неизвестная причина изменения отпечатка файла {meta_file.name}:\n\
                                 {db_meta['fingerprint']} => {meta_file.fingerprint}")
        # Если файла нет в БД — дорабатываем его и
        # создаём событие FileCreatedEvent без формирования метаданных
        else:
            event = FileCreatedEvent(
                                     src_path=file.as_posix(),
                                     dest_path='',
                                     is_synthetic=False
                                    )
            self.event_handler.on_created(event=event, meta_prepared=True)
        # Дорабатываем мету файла
        meta_file.finalize()                
        # Сохраняем мету файла в списке новых файлов
        self.new_indexed_files.update({meta_id:meta_file})
        return meta_file


    def update_batch_metadata(
                              self,
                              meta_file:SourceFileMeta,
                              update_during_init:bool
                             ) -> None:
        """
        Формируем метаданные батча: 
        если батч не существует — создаём его;
        если есть — добавляем файл в него;
        если батч был в БД - генерируем BatchMeta и обновляем его
        :param meta_file: объект SourceFileMeta
        :return: словарь батчей
        """
        # Добавляем записи о батче и образце, к которым относится файл
        # В этой же функции батч/образец добавляются в соответствующие списки
        name = meta_file.batch
        meta_file_id = meta_file.symlink.as_posix()
        # Для начала проверяем, есть ли батч в обновлённых батчах
        if name in self.new_indexed_batches.keys():
            meta_batch = self.new_indexed_batches[name]
        # В противном случае — проверяем, есть ли он в БД
        else:
            # Смотрим в коллекции загруженных батчей
            if name in self.loaded_batches.keys():
                pass
            else:
                # если и там нет — пробуем загрузить из БД
                doc = self.dao.find_one(
                                    collection="batches",
                                    query={"name": name},
                                    projection=None  # Загружаем все поля
                                )
                if doc:
                    # Найден в БД? Преобразуем в объект BatchMeta
                    self.loaded_batches[name] = BatchMeta.from_db(doc)
                else:
                    self.loaded_batches[name] = None
            meta_batch = self.loaded_batches[name]
            # Если батч успешно выгружен из БД — работаем с ним
            if meta_batch:
                # Если батч был обновлён — сохраняем его в new_indexed_batches
                if meta_batch.update_batch(
                                           meta_file.symlink,
                                           self.file_diffs[meta_file_id]
                                          ):
                    self.new_indexed_batches[name] = meta_batch
            # Если же батч не был выгружен из БД (и в new_indexed_batches его тоже нет) — создаём его
            else:
                meta_batch = BatchMeta(
                                       name=name,
                                       final_summary=next((s for s
                                                           in self.summaries['final_summary']
                                                           if name in s.name),
                                                          Path()
                                                         ),
                                       sequencing_summary=next((s for s
                                                                in self.summaries['sequencing_summary']
                                                                if name in s.name),
                                                               Path()
                                                              )
                                      )
                
        # Отлично, мета батча в нашем распоряжении; теперь разберёмся, что мы делаем:
        # добавляем мету файла, модифицируем её или удаляем полностью    
                meta_batch.update_batch(
                                        meta_file.symlink,
                                        self.file_diffs[meta_file_id]
                                       )
                self.new_indexed_batches[name] = meta_batch





            meta_batch = self.new_indexed_batches.get(name)
            # Если батча нет в списке — создаём его
            if not meta_batch:
                meta_batch = BatchMeta(
                            name=name,
                            final_summary=next((s for s
                                                in self.summaries['final_summary']
                                                if name in s.name), Path()),
                            sequencing_summary=next((s for s
                                                    in self.summaries['sequencing_summary']
                                                    if name in s.name), Path()))
            # Добавляем файл в мету батча
            meta_batch.add_file2batch(meta_file)
            # Обновляем батч в списке батчей
            self.new_indexed_batches[name] = meta_batch
        return meta_batch
    

    def complete_batch_metadata(self, meta_batch:BatchMeta) -> None:
        # Создаём отпечатки для каждого батча и определяем статус
        meta_batch.finalize()
        # Если до этого поймали 'curation' - отправляем его на курацию
        if meta_batch.status == 'curation':
            self.curations[meta_batch.name] = meta_batch
            # !!! TO-DO Сформировать уведомление о необходимости курации
        # Если при индексации была сформирована новая версия батча — помечаем старую версию как 'deprecated'


def form_sample_metadata(meta_batch:BatchMeta,
                        samples: Dict[str, SampleMeta],
                        files: Dict[str, SourceFileMeta]
                       ) -> Dict[str, SampleMeta]:
    samples = upsert_samples(samples=samples,
                             src=meta_batch,
                             files_meta=files)
    return samples


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


def replace_symlink_source(symlink: Path, new_source: Path) -> None:
    """
    Если симлинк существует, то меняем его источник
    """
    if symlink.exists():
        if symlink.is_symlink():
            symlink.unlink()
            symlink.symlink_to(new_source)
        else:
            raise FileExistsError(f"{symlink} is not a symlink")


def obj_in_dict(obj: Any, dict_: Dict[Any, Any]) -> bool:
    """
    Проверяет, что объект obj есть в словаре dict_

    :param obj: объект
    :return: True, если объект есть в словаре
    """
    if obj in dict_.keys():
        return True
    else:
        return False


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

