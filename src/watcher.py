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

from utils.common import load_yaml
from utils.db.db import ConfigurableMongoDAO, _load_db_config_yaml, get_mongo_client
from utils.filesystem.crawler import FsCrawler
from utils.scheduler.task_scheduler import TaskScheduler
from utils.logger import get_logger

logger = get_logger(name=__name__)

@flow(name="Initialization")
def stage_init(
               cfg:Dict[str, Any],
               pipeline_cfg:Dict[str, Dict[str, Any]]
              ) -> tuple:
    """Subflow: .env → DB → FS → Scheduler"""
    dao = init_db(cfg)
    fs_watcher = init_fs(cfg, dao)
    scheduler = init_scheduler(cfg, pipeline_cfg)
    return (fs_watcher, scheduler)

# инициализация БД (DAO) + индексы
@task(cache_policy=NO_CACHE, persist_result=False)
def init_db(
                 cfg:Dict[str, Any]
                ) -> ConfigurableMongoDAO:
    db_cfg_path = Path(cfg["db_config_path"] or Path(__file__).parent / "config/db_config.yaml")
    collections_cfg = _load_db_config_yaml(db_cfg_path)
    db_uri:str = cfg.get("db_uri", "")
    db_user:str = cfg.get("db_user", "")
    db_pass:str = cfg.get("db_pass", "")
    db_name:str = cfg.get("db_name", "")
    client = get_mongo_client(db_uri, db_user, db_pass, db_name)
    db_name = cfg["db_name"]
    dao = ConfigurableMongoDAO(client=client, db_name=db_name, collections_config=collections_cfg)
    # !!! добавить при инициализации проверку наличия коллекций в БД:
    # files, batches, curations, samples, tasks, results
    # А также проверку их индексов
    dao.init_dao()
    return dao

# Инициализация вотчдога ФС
@task
def init_fs(
                 dao:ConfigurableMongoDAO,
                 cfg:Dict[str, Any]
                ) -> FsCrawler:
    src = Path(cfg.get("source_dir", "."))
    link = Path(cfg.get("link_dir", "./links"))
    out = Path(cfg.get("result_dir", "./results"))
    tmp = Path(cfg.get("tmp_dir", "./tmp"))
    db_writing_debounce = cfg.get("debounce", 10)
    file_modified_debounce = cfg.get("file_modification_debounce", 5)
    db_update_interval = cfg.get("db_update_interval", 60)
    for p in (link, out, tmp):
        p.mkdir(parents=True, exist_ok=True)

    exts_raw = cfg.get("source_files_extensions") or ""
    extensions = tuple([e.strip() for e in exts_raw.split(",") if e.strip()])

    crawler = FsCrawler(
                        source_dir=src,
                        link_dir=link,
                        dao=dao,
                        filetypes=extensions,
                        db_debounce=db_writing_debounce,
                        file_modified_debounce=file_modified_debounce,
                        db_update_interval=db_update_interval
                       )
    crawler.init_crawler()
    return crawler

@task
# Инициализация планировщика задач
def init_scheduler(
                   dao:ConfigurableMongoDAO,
                   cfg:Dict[str, Any],
                   pipeline_cfg:Dict[str, Dict[str, Any]]
                  ) -> TaskScheduler:
    slurm_poll_interval = cfg.get("slurm_poll_interval", 60)
    scheduler = TaskScheduler(dao, slurm_poll_interval, pipeline_cfg)
    scheduler.init_scheduler()
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
    # Получаем конфиг
    cfg = load_yaml(
                    Path(__file__).parent / "config/config.yaml",
                    critical=True
                   )
    pipeline_cfg = load_yaml(
                             Path(__file__).parent / "config/pipeline_config.yaml",
                             critical=True
                            ) 
    # Инициируем системы
    fs_watcher, dao, scheduler = stage_init(cfg, pipeline_cfg)
    # Запуск вотчдога для мониторинга событий ФС и постановки задач для обработки
    run_watchdog(fs_watcher, dao, scheduler)

if __name__ == "__main__":
    main()
