# -*- coding: utf-8 -*-
"""
Главный флоу Prefect: инициализация → индексация → запись в БД → постановка задач в Slurm
"""
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from dataclasses import fields, is_dataclass
from typing import Any, Dict, List

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from utils.common import load_yaml, setup_parser
from utils.db.db import ConfigurableMongoDAO, _load_db_config_yaml, get_mongo_client
from utils.filesystem.crawler import FsCrawler
from utils.scheduler.task_scheduler import TaskScheduler
from utils.logger import get_logger

logger = get_logger(name=__name__)

@flow(name="Initialization")
def stage_init(
               cfg:Dict[str, Dict[str, Any]]
              ) -> tuple:
    """Subflow: config → DB → FS → Scheduler"""
    dao = init_db(cfg['database'])
    fs_watcher = init_fs(dao, cfg['filesystem'])
    scheduler = init_scheduler(dao, cfg['scheduler'], cfg['pipelines'])
    return (fs_watcher, dao, scheduler)

# инициализация БД (DAO) + индексы
@task(cache_policy=NO_CACHE, persist_result=False)
def init_db(
            db_cfg:Dict[str, Any]
           ) -> ConfigurableMongoDAO:
    dao = ConfigurableMongoDAO(db_cfg)
    dao.init_dao()
    return dao

# Инициализация вотчдога ФС
@task
def init_fs(
            dao:ConfigurableMongoDAO,
            cfg:Dict[str, Any]
           ) -> FsCrawler:
    crawler = FsCrawler(cfg, dao)
    crawler.init_crawler()
    return crawler

@task
# Инициализация планировщика задач
def init_scheduler(
                   dao:ConfigurableMongoDAO,
                   scheduler_cfg:Dict[str, Any],
                   pipeline_cfg:Dict[str, Dict[str, Any]]
                  ) -> TaskScheduler:
    slurm_poll_interval = scheduler_cfg.get("slurm_poll_interval", 60)
    scheduler = TaskScheduler(dao, slurm_poll_interval)
    scheduler.init_scheduler(pipeline_cfg)
    return scheduler

@flow(name="Watchdog")
def run_watchdog(
                 crawler:FsCrawler,
                 dao:ConfigurableMongoDAO,
                 scheduler:TaskScheduler
                ) -> None:
    # Запуск мониторинга ФС
    crawler.start_crawler()
    # Запуск мониторинга БД
    dao.start_dao()
    # Запуск планировщика задач
    scheduler.start_scheduler()
    return

@flow(name="Main")
def main() -> None:
    cfg_path:Path = setup_parser().parse_args().configuration
    # Получаем конфиг
    cfg: Dict[str, Dict[str, Any]] = load_yaml(
                                               cfg_path,
                                               critical=True
                                              )
    # Инициируем системы
    fs_watcher, dao, scheduler = stage_init(cfg)
    # Запуск вотчдога для мониторинга событий ФС и постановки задач для обработки
    try:
        run_watchdog(fs_watcher, dao, scheduler)
    except KeyboardInterrupt:
        logger.debug("Watchdog stopped by user KeyboardInterrupt")
        logger.info("Shutting down...")
    finally:
        fs_watcher.stop_crawler()
        scheduler.stop_scheduler()
        dao.stop_dao()

if __name__ == "__main__":
    main()
