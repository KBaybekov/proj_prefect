# -*- coding: utf-8 -*-
"""
Оркестрирует запуск всех компонентов системы:
- Подключение к MongoDB
- Инициализация мониторинга файловой системы
- Запуск планировщика задач (TaskScheduler)
- Обработка событий и постановка работ в Slurm

Использует Prefect для управления потоком выполнения и жизненным циклом сервисов.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Tuple

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from utils.common import load_yaml, setup_parser
from utils.db.db import ConfigurableMongoDAO
from utils.filesystem.crawler.fs_crawler import FsCrawler
from utils.scheduler.managers.task_scheduler import TaskScheduler
from utils.logger import get_logger

logger = get_logger(name=__name__)

@flow(name="Initialization")
def stage_init(
               cfg:Dict[str, Dict[str, Any]]
              ) -> Tuple[FsCrawler, ConfigurableMongoDAO, TaskScheduler]:
    """
    Подфлоу инициализации всех компонентов системы.

    Выполняет последовательную инициализацию:
    1. Подключение к базе данных (DAO)
    2. Настройку мониторинга файловой системы (FsCrawler)
    3. Инициализацию планировщика задач (TaskScheduler)

    :param cfg: Полный конфигурационный словарь, содержащий секции 'database', 'filesystem', 'scheduler'.
    :type cfg: Dict[str, Dict[str, Any]]
    :return: Кортеж из инициализированных объектов: (FsCrawler, ConfigurableMongoDAO, TaskScheduler).
    :rtype: Tuple[FsCrawler, ConfigurableMongoDAO, TaskScheduler]
    """
    dao = init_db(cfg['database'])
    fs_watcher = init_fs(dao, cfg['filesystem'])
    scheduler = init_scheduler(dao, cfg['scheduler'])
    return (fs_watcher, dao, scheduler)

# инициализация БД (DAO) + индексы
@task(cache_policy=NO_CACHE, persist_result=False)
def init_db(
            db_cfg:Dict[str, Any]
           ) -> ConfigurableMongoDAO:
    """
    Инициализирует подключение к MongoDB и создаёт необходимые индексы.

    Выполняется как Prefect-задача без кэширования.

    :param db_cfg: Конфигурация подключения к базе данных.
                   Должна содержать: host, port, db_name, коллекции и т.д.
    :type db_cfg: Dict[str, Any]
    :return: Инициализированный объект ConfigurableMongoDAO для взаимодействия с БД.
    :rtype: ConfigurableMongoDAO
    """
    dao = ConfigurableMongoDAO(db_cfg)
    dao.init_dao()
    return dao

# Инициализация вотчдога ФС
@task
def init_fs(
            dao:ConfigurableMongoDAO,
            cfg:Dict[str, Any]
           ) -> FsCrawler:
    """
    Инициализирует FsCrawler для мониторинга файловой системы.

    Создаёт объект FsCrawler, настраивает директории, запускает первичную индексацию файлов.

    :param dao: Объект ConfigurableMongoDAO для доступа к данным в БД.
    :type dao: ConfigurableMongoDAO
    :param cfg: Конфигурация файлового мониторинга (пути, расширения, тайминги).
    :type cfg: Dict[str, Any]
    :return: Готовый к работе экземпляр FsCrawler.
    :rtype: FsCrawler
    """
    crawler = FsCrawler(cfg, dao)
    crawler.init_crawler()
    return crawler

@task
# Инициализация планировщика задач
def init_scheduler(
                   dao:ConfigurableMongoDAO,
                   scheduler_cfg:Dict[str, Any]
                  ) -> TaskScheduler:
    """
    Инициализирует планировщик задач для запуска пайплайнов обработки.

    Настраивает соединение с системой заданий (например, Slurm), регистрирует пайплайны.

    :param dao: Объект ConfigurableMongoDAO для сохранения состояния задач.
    :type dao: ConfigurableMongoDAO
    :param scheduler_cfg: Конфигурация планировщика (тип, параметры подключения, пути).
    :type scheduler_cfg: Dict[str, Any]
    :return: Инициализированный объект TaskScheduler.
    :rtype: TaskScheduler
    """
    scheduler = TaskScheduler(scheduler_cfg, dao)
    scheduler.init_scheduler()
    return scheduler

@flow(name="Watchdog")
def run_watchdog(
                 crawler:FsCrawler,
                 dao:ConfigurableMongoDAO,
                 scheduler:TaskScheduler
                ) -> None:
    """
    Основной флоу мониторинга: запускает все компоненты в режиме ожидания событий.

    - FsCrawler начинает наблюдать за изменениями в файловой системе
    - DAO запускает фоновые процессы синхронизации с БД
    - TaskScheduler начинает проверку новых задач для запуска

    Работает бесконечно до получения сигнала остановки.

    :param crawler: Экземпляр FsCrawler для отслеживания файлов.
    :type crawler: FsCrawler
    :param dao: Экземпляр DAO для взаимодействия с базой данных.
    :type dao: ConfigurableMongoDAO
    :param scheduler: Экземпляр планировщика для постановки задач.
    :type scheduler: TaskScheduler
    """
    # Запуск мониторинга ФС
    crawler.start_crawler()
    # Запуск мониторинга БД
    dao.start_dao()
    # Запуск планировщика задач
    scheduler.start_scheduler()
    return

@flow(name="Main")
def main() -> None:
    """
    Главная точка входа в приложение.

    Выполняет:
    1. Парсинг аргументов командной строки
    2. Загрузку конфигурации из YAML-файла
    3. Инициализацию всех компонентов (подфлоу stage_init)
    4. Запуск основного цикла мониторинга (run_watchdog)
    5. Корректное завершение при прерывании (Ctrl+C)

    Обеспечивает отказоустойчивый жизненный цикл сервиса.
    """
    cfg_path:Path = setup_parser().parse_args().configuration
    # Получаем конфиг
    cfg: Dict[str, Dict[str, Any]] = load_yaml(
                                               cfg_path,
                                               critical=True
                                              )
    # Инициируем системы
    fs_crawler, dao, scheduler = stage_init(cfg)
    # Запуск вотчдога для мониторинга событий ФС и постановки задач для обработки
    try:
        run_watchdog(
                     fs_crawler,
                     dao,
                     scheduler
                    )
    except KeyboardInterrupt:
        logger.debug("Watchdog stopped by user KeyboardInterrupt")
        logger.info("Shutting down...")
    finally:
        fs_crawler.stop_crawler()
        scheduler.stop_scheduler()
        dao.stop_dao()

if __name__ == "__main__":
    main()
