from prefect import task, flow
from prefect.testing.utilities import prefect_test_harness
import logging
import sys
from typing import Any, Dict
from pathlib import Path
from dotenv import load_dotenv
from utils.common import env_var
from utils.db import check_db_connection, MongoDAO
from utils.filesystem import check_fs_ok, index_source_files
from utils.scheduler import connect_scheduler

"""
STAGE_0: Инициализация
"""

@task
def load_env():
    """Загрузка .env"""
    try:
        load_dotenv()
        logger.info(".env успешно загружен")
    except Exception:
        logger.exception("Ошибка при загрузке .env")
        raise


@task
def init_db(fs_data:dict) -> Dict[str, Any]:
    """Проверка БД"""
    db_server = check_db_connection(
                                    host=env_var('DB_HOST'),
                                    db_user=env_var('DB_USER'),
                                    db_pass=env_var('DB_PASSWORD'),
                                    db_timeout=env_var('DB_TIMEOUT')
                                   )
    logger.info("БД доступна.")
    # Получаем объект DAO для операций с БД
    db = MongoDAO(client=db_server, db_name=env_var('DB_NAME'), cfg_path=Path(env_var('DB_CONFIG_PATH')))
    # Подгружаем снапшоты папок с исходниками, полученные при прошлой индексации ФС
    dir_snapshots = db.load_dir_snapshots_from_db()
    return {'db':db, 'dir_snapshots':dir_snapshots}


@task
def init_fs(dir_snapshots:dict) -> Dict[str, Any]:
    """Инициализация файловой системы"""
    in_d, link_d, out_d, tmp_d = check_fs_ok(
                                             in_d=env_var('SOURCE_DIR'),
                                             link_d=env_var('LINK_DIR'),
                                             out_d=env_var('RESULT_DIR'),
                                             tmp_d=env_var('TMP_DIR')
                                             )
    logger.info("ФС доступна")
        
    # Первичный обход входной папки,
    # формирование метадаты исходных файлов,
    # при необходимости - создание симлинка с именем нужного формата 
    filetypes = tuple(env_var('SOURCE_FILES_EXTENSIONS').split(','))
    fs_index = index_source_files(
                               source_dir=in_d,
                               link_dir=link_d,
                               filetypes=filetypes,
                               dir_snapshots=dir_snapshots
                              )
    logger.info(f"Проиндексировано {len(fs_index[0].keys())} файлов в {in_d}")
    logger.info("ФС: OK")
    fs_data = {
               'in_d':in_d,
               'link_d':link_d,
               'out_d':out_d,
               'tmp_d':tmp_d,
               'fs_index':fs_index
              }
    return fs_data


@task
def init_scheduler():
    """Проверка планировщика"""
    try:
        scheduler = connect_scheduler()
        logger.info("Планировщик доступен")
        return scheduler
    except Exception as e:
        logger.exception("Ошибка подключения планировщика")
        raise


@flow(name="Initialization")
def stage_init() -> dict:
    """
    Subflow, объединяющий этап инициализации:
    .env → DB → FileSystem → Scheduler → Environment Data
    """
    init_data:Dict[str, Any] = {}
    # Загрузка переменных из .env
    load_env()
    # БД
    # Получаем клиент Mongo и снапшоты сканированных ранее папок
    db_data = init_db()
    init_data.update(db_data)
    # Файловая система
    # Получаем имена папок и проиндексированные файлы/папки/батчи/образцы
    fs_data = init_fs(init_data['dir_snapshots'])
    init_data.update(fs_data)
    # планировщик заданий
    scheduler = init_scheduler()
    init_data.update({'scheduler':scheduler})
    # Возвращаем всё сразу словарём
    return init_data


@flow(name="Main Pipeline")
def main_flow():
    init_data = stage_init()
    logger.info(f"Initialization complete")
    while True:
        samples = pick_samples(dao)
        logger.info(f"Samples tratatata")

        submits = submit_jobs(dao, samples)
        logger.info(f"Jobs submittted tratatata")

        record_results(dao, submits)
        logger.info(f"Records tratatata")
        
        logger.info(f"Sleeping tratatata")
        time.sleep(xxx)

if __name__ == "__main__":
    # Логирование
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger = logging.getLogger(__name__)

    main_flow()