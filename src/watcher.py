# -*- coding: utf-8 -*-
"""
Главный флоу Prefect: инициализация → индексация → запись в БД → постановка задач в Slurm
"""
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from dataclasses import fields, is_dataclass
from typing import Any, Dict

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from utils.common import load_yaml
from utils.db import init_db, ConfigurableMongoDAO
from utils.filesystem.crawler import FsCrawler
from utils.scheduler import check_slurm, submit_sample_job, TaskScheduler
from utils.logger import get_logger

from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler
from watchdog.observers import Observer

logger = get_logger(name=__name__)

# инициализация БД (DAO) + индексы
@task(cache_policy=NO_CACHE, persist_result=False)
def init_db_task(cfg:Dict[str, Any]):
    dao = init_db(cfg)
    return dao


# индексация ФС
@task
def init_fs_task(dao:ConfigurableMongoDAO, cfg:Dict[str, Any]) -> FsCrawler:
    src = Path(cfg.get("source_dir", "."))
    link = Path(cfg.get("link_dir", "./links"))
    out = Path(cfg.get("result_dir", "./results"))
    tmp = Path(cfg.get("tmp_dir", "./tmp"))
    debounce = cfg.get("debounce", 10)
    for p in (link, out, tmp):
        p.mkdir(parents=True, exist_ok=True)

    exts_raw = cfg.get("source_files_extensions") or ""
    extensions = tuple([e.strip() for e in exts_raw.split(",") if e.strip()])

    crawler = FsCrawler(
                        source_dir=src,
                        link_dir=link,
                        dao=dao,
                        filetypes=extensions,
                        debounce=debounce
                       )
    return crawler


# проверка планировщика
@task
def init_scheduler_task():
    return check_slurm()


def _public_dataclass_dict(obj) -> Dict[str, Any]:
    """Собирает словарь только из атрибутов датакласса, чьи имена не начинаются с '_'."""
    if not is_dataclass(obj):
        raise TypeError(f"Expected dataclass, got {type(obj)!r}")
    data = {}
    for f in fields(obj):
        name = f.name
        if name.startswith("_"):
            continue
        data[name] = getattr(obj, name)
    return data


@task(cache_policy=NO_CACHE, persist_result=False)
def persist_index(dao: ConfigurableMongoDAO, index: Dict[str, Dict]) -> None:
    now = datetime.now(timezone.utc)

    for collection in ['files', 'batches', 'samples', 'curations']:
        for meta in index[collection].values():
            # превращаем класс и все вложенные атрибуты в словарь,
            # который можно будет скормить Mongo
            doc = _public_dataclass_dict(meta)
            # Доп. служебные поля
            doc["updated_at_DB"] = now
            # Добавление в БД
            dao.upsert(collection, {"name": doc['name']}, doc)


# GPT5: выбор и запуск задач обработки (не ждём завершения)
# -----------------------------------------------------------------------------
@task(cache_policy=NO_CACHE, persist_result=False)
def select_and_submit(dao, paths: dict) -> None:
    ready = dao.find("samples", {"status": "ready"})
    for s in ready:
        sample = s["sample"]
        batch = s["batch"]
        job_id = submit_sample_job(sample, str(paths["src"]), str(paths["out"]), str(paths["tmp"]))
        dao.upsert("samples", {"batch": batch, "sample": sample}, {"status": "submitted", "job_id": job_id})


@task(cache_policy=NO_CACHE, persist_result=False)
def collect_results(dao, out_dir: Path) -> None:
    submitted = dao.find("samples", {"status": "submitted"})
    for s in submitted:
        sample = s["sample"]
        done = out_dir / sample / f"{sample}.done"
        if done.exists():
            # пишем в human и отмечаем завершение
            dao.upsert("human", {"sample": sample}, {"sample": sample, "result": "OK"})
            dao.upsert("samples", {"batch": s["batch"], "sample": sample}, {"status": "completed"})


# subflow и основной flow
@flow(name="Initialization")
def stage_init(cfg:Dict[str, Any]):
    """Subflow: .env → DB → FS → Scheduler"""
    dao = init_db_task(cfg)
    fs_watcher = init_fs_task(cfg, dao)
    scheduler = init_scheduler_task(cfg)
    # Идемпотентно подгружаем проиндексированные данные в БД
    persist_index(dao, {k: fs_data[k] for k in  # type: ignore
                                       ("files",
                                        "batches",
                                        "curations",
                                        "samples",
                                        "directories")
                        }
                 )
    return {"dao": dao, **fs_data, "scheduler": scheduler} # type: ignore


@flow(name="Watchdog")
def run_watchdog(init:Dict[str, Any]) -> None:
    fs_watcher = FsCrawler(
                           patterns=[f"*.{ext}" for ext in init["extensions"]],
                           ignore_directories=True,
                           case_sensitive=False
                          )
    
    
    


@flow(name="Main")
def main() -> None:
    dao: ConfigurableMongoDAO
    # Получаем конфиг
    cfg = load_yaml(Path(__file__).parent / "config/config.yaml",
                    critical=True)
    # Инициируем системы
    init = stage_init(cfg)
    # Получаем обёртки для работы с БД и планировщиком, а также рабочие папки
    dao = init["dao"]
    scheduler = init["scheduler"]
    paths = {p:init[p] for p in
                       ["src",
                        "link",
                        "out",
                        "tmp"]
            }
    
    # Запуск вотчдога для мониторинга событий ФС,
    # постановки задач для обработки и курации БД
    run_watchdog(init)


if __name__ == "__main__":
    main()
