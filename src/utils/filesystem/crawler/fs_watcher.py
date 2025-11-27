# -*- coding: utf-8 -*-
"""
Модуль FsWatcher — это обработчик событий файловой системы, построенный на основе
библиотеки watchdog. Он отслеживает создание, изменение, перемещение и удаление файлов,
синхронизирует состояние файлов с метаданными в MongoDB и управляет симлинками.
"""
from . import remove_symlink
import os
import threading
from utils.logger import get_logger
from datetime import datetime
from typing import (
                    Dict,
                    List,
                    Tuple,
                    TYPE_CHECKING,
                    Union
                   )
from pathlib import Path

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

from utils.filesystem.metas import SourceFileMeta


logger = get_logger(__name__)

if TYPE_CHECKING:
    from .fs_crawler import FsCrawler

class FsWatcher(PatternMatchingEventHandler):
    """
    Обработчик событий файловой системы, отслеживающий изменения файлов и директорий.

    Интегрируется с FsCrawler для:
    - Создания и обновления симлинков
    - Сбора и синхронизации метаданных файлов
    - Обнаружения перемещений и изменений содержимого
    - Управления жизненным циклом файлов, батчей и образцов

    Работает с событиями только файлов (директории игнорируются).
    """
    def __init__(
                 self,
                 crawler:"FsCrawler",
                 patterns: List[str],
                 ignore_directories: bool,
                 case_sensitive: bool
                ):
        """
        Инициализирует обработчик событий.

        :param crawler: Ссылка на экземпляр FsCrawler для взаимодействия.
        :type crawler: FsCrawler
        :param patterns: Список шаблонов имён файлов для отслеживания (например, "*.fastq").
        :type patterns: List[str]
        :param ignore_directories: Игнорировать ли события от директорий.
        :type ignore_directories: bool
        :param case_sensitive: Учитывать ли регистр при сопоставлении имён.
        :type case_sensitive: bool
        """
        super().__init__(
                         patterns=patterns,
                         ignore_directories=ignore_directories,
                         case_sensitive=case_sensitive
                        )
        self.crawler:FsCrawler = crawler
        """
        Ссылка на экземпляр FsCrawler.

        Позволяет обработчику событий взаимодействовать с основным классом,
        вызывать его методы (например, update_batch_metadata, _extract_file_info_from_db)
        и получать доступ к его состоянию (буферы, директории, конфигурация).
        Устанавливается при инициализации и используется во всех методах обработки событий.
        """
        
    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        """
        Обрабатывает событие создания объекта в файловой системе.

        Для файлов вызывает общий обработчик `handle_file_event`.
        События директорий только логируются.

        :param event: Событие создания.
        """
        if isinstance(event, DirCreatedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        elif isinstance(event, FileCreatedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
            self.handle_file_event(event)
        return None
    
    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        """
        Обрабатывает событие перемещения объекта в файловой системе.

        Для файлов вызывает общий обработчик `handle_file_event`.
        События директорий только логируются.

        :param event: Событие перемещения.
        """
        if isinstance(event, DirMovedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        elif isinstance(event, FileMovedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
            self.handle_file_event(event)
        return None
    
    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        """
        Обрабатывает событие изменения объекта в файловой системе.

        Для файлов вызывает общий обработчик `handle_file_event`.
        События директорий только логируются.

        :param event: Событие изменения.
        """
        if isinstance(event, DirModifiedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
        elif isinstance(event, FileModifiedEvent):
            logger.debug(f"Watchdog: {type(event)}. Путь: {event.src_path}")
            self.handle_file_event(event)
        return None
    
    def on_deleted(self, event: DirDeletedEvent | FileDeletedEvent) -> None:
        """
        Обрабатывает событие удаления объекта из файловой системы.

        Для файлов вызывает общий обработчик `handle_file_event`.
        События директорий только логируются.

        :param event: Событие удаления.
        """
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
        """
        Централизованный обработчик всех событий, связанных с файлами.

        Определяет тип события и выполняет соответствующие действия:
        - Создание симлинков и метаданных
        - Проверка изменений содержимого (по отпечатку)
        - Обновление путей при перемещении
        - Пометка файлов как удалённых
        - Управление дебаунсом при модификации

        :param event: Событие, связанное с файлом.
        :type event: Union[FileCreatedEvent, FileModifiedEvent, FileMovedEvent, FileDeletedEvent]
        :param kwargs: Дополнительные параметры (например, `meta_file`, `db_meta`, `found_during_init`).
        """
        def _is_file_moved(
                           db_meta:Dict[str, datetime | str | int | None],
                           meta_file:SourceFileMeta
                          ) -> bool:
            """
            Проверяет, был ли файл перемещён, сравнивая путь в ФС с записью в БД.

            :param db_meta: Метаданные файла из БД.
            :type db_meta: Dict[str, Union[datetime, str, int, None]]
            :param meta_file: Метаданные файла из файловой системы.
            :type meta_file: SourceFileMeta
            :return: True, если путь к файлу изменился.
            :rtype: bool
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
                    Определяет тип отчёта по имени файла.

                    :param file: Путь к файлу отчёта.
                    :type file: Path
                    :return: Ключ типа отчёта (например, 'final_summary'), или пустая строка.
                    :rtype: str
                    """
                    for summary_type in self.crawler.summaries.keys():
                        if summary_type in file.name:
                            return summary_type
                    return ""
                
                def __add_summary_2_collection(
                                               file:Path,
                                               summary_type:str
                                              ) -> None:
                    """Добавляет файл отчёта в соответствующую коллекцию."""

                    if file not in self.crawler.summaries[summary_type]:
                        self.crawler.summaries[summary_type].add(file)
                    return None
                
                def __remove_summary_from_collection(
                                                     file:Path,
                                                     summary_type:str
                                                    ) -> None:
                    """Удаляет файл отчёта из соответствующей коллекции."""

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
                    
            # === Обработка событий для файлов данных ===

            # --- СОЗДАНИЕ ФАЙЛА ---
            if isinstance(event, FileCreatedEvent):
                def __create_meta_n_symlink(
                                           filepath:Path,
                                           symlink_dir:Path
                                           ) -> Tuple[SourceFileMeta, str]:
                    """
                    Создаёт метаданные файла и симлинк в указанной директории.

                    :param filepath: Абсолютный путь к исходному файлу.
                    :type filepath: Path
                    :param symlink_dir: Директория для создания симлинка.
                    :type symlink_dir: Path
                    :return: Кортеж (SourceFileMeta, идентификатор_ссылки).
                    :rtype: Tuple[SourceFileMeta, str]
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
                        Сравнивает отпечаток файла с сохранённым в БД.

                        :param db_meta: Метаданные из БД.
                        :type db_meta: Dict[str, Union[datetime, str, int, None]]
                        :param meta_file: Новые метаданные.
                        :type meta_file: SourceFileMeta
                        :return: True, если файл не изменился.
                        :rtype: bool
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
            
            # --- ИЗМЕНЕНИЕ ФАЙЛА ---
            elif isinstance(event, FileModifiedEvent):
                def __register_file_changes(
                                            db_meta:Dict[str, datetime | str | int | None],
                                            meta_file:SourceFileMeta,
                                            meta_id:str
                                           ) -> None:
                    """
                    Фиксирует изменения в файле и обновляет метаданные.
                    """
                    
                    def ___extract_file_changes(
                                                props2check:List[str],
                                                db_meta:Dict[str, datetime | str | int | None],
                                                meta_file:SourceFileMeta
                                               ) -> Dict[str, List[str|int|datetime]]:
                        """Определяет, какие свойства файла изменились."""

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
                                   
            # --- ПЕРЕМЕЩЕНИЕ ФАЙЛА ---
            elif isinstance(event, FileMovedEvent):
                def replace_symlink_source(symlink: Path, new_source: Path) -> None:
                    """
                    Обновляет симлинк, указывающий на перемещённый файл.
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
            
            # --- УДАЛЕНИЕ ФАЙЛА ---
            elif isinstance(event, FileDeletedEvent):
                db_meta = kwargs.get("db_meta",
                                     self.crawler._extract_file_info_from_db(
                                                                             meta_field="filepath",
                                                                             meta_id=str(event.src_path),
                                                                             additional_fields=[
                                                                                                'batch',
                                                                                                'sample',
                                                                                                'fingerprint',
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
        Определяет директорию для создания симлинка на основе расширения файла.

        :param filepath: Путь к исходному файлу.
        :type filepath: Path
        :return: Путь к директории, где должен быть создан симлинк.
        :rtype: Path
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
