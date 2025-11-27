from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
from yaml import dump, safe_load, YAMLError

from utils.db.db import ConfigurableMongoDAO
from utils.filesystem.crawler.fs_crawler import FsCrawler
from utils.scheduler.managers.task_scheduler import TaskScheduler
from utils.logger import get_logger

logger = get_logger(name=__name__)

def setup_parser() -> ArgumentParser:
    """
    Создаёт и настраивает парсер аргументов командной строки.

    Определяет обязательные и опциональные параметры запуска, предоставляет
    подробное описание и примеры использования.

    :return: Настроенный экземпляр ArgumentParser.
    :rtype: ArgumentParser
    """
    parser = ArgumentParser(
                                     description="FsCrawler - инструмент для сканирования файловой системы и работы с метаданными",
                                     formatter_class=RawDescriptionHelpFormatter,
                                     epilog="""
                                               Примеры использования:
                                               %(prog)s scan --source /data --link /links --db_uri mongodb://localhost:27017
                                               %(prog)s index --source /data --link /links
                                               %(prog)s watch --source /data --polling
                                            """
                                )

    # Основные аргументы
    parser.add_argument(
                        "-c",
                        "--configuration",
                        type=Path,
                        required=True,
                        help="Файл конфигурации"
                       )
    return parser

def load_yaml(
              file_path:Path,
              encoding:str = "utf-8",
              critical:bool = False,
              subsection:str = ''
             ) -> dict:
    """
    Загружает данные из YAML-файла в виде словаря.

    Безопасно парсит YAML с использованием safe_load. Поддерживает загрузку
    всего файла или конкретной секции. Обрабатывает все возможные ошибки
    (кодировка, синтаксис, отсутствие файла) и ведёт логирование.

    :param file_path: Путь к YAML-файлу, который необходимо загрузить.
    :type file_path: Path
    :param encoding: Кодировка файла. По умолчанию — 'utf-8'.
    :type encoding: str
    :param critical: Если True — при ошибке будет поднято исключение.
                     Если False — функция вернёт пустой словарь.
    :type critical: bool
    :param subsection: Имя секции в YAML, которую нужно загрузить. Если не указано,
                       возвращается весь документ.
    :type subsection: str
    :return: Словарь с загруженными данными. Может быть пустым.
    :rtype: dict
    :raises FileNotFoundError: Если critical=True и файл не найден.
    :raises UnicodeDecodeError: Если critical=True и ошибка кодировки.
    :raises YAMLError: Если critical=True и ошибка синтаксиса YAML.
    :raises KeyError: Если critical=True и указанная секция не найдена.
    """

    data: dict = {}
    # Открываем YAML-файл для чтения
    try:
        data = safe_load(file_path.read_text(encoding=encoding)) or {}  # Загружаем содержимое файла в словарь с помощью safe_load
        if subsection:
            try:
                data = data[subsection]
            except KeyError:
                logger.error(f"Раздел '{subsection}' не найден в {file_path}")
                if critical:
                    raise KeyError
    except UnicodeDecodeError:
        logger.error(f"Ошибка кодировки файла. Проверьте кодировку: {file_path}.")
        if critical:
            raise UnicodeDecodeError # type: ignore
    except YAMLError:
        logger.error(f"Ошибка парсинга YAML файла: {file_path}")
        if critical:
            raise YAMLError
    except FileNotFoundError:
        logger.error(f"Файл не найден: {file_path}")
        if critical:
            raise FileNotFoundError
    except Exception as e:
        logger.error(f"Ошибка при открытии YAML файла {file_path}:\n%s",
                     e, exc_info=True)
        if critical:
            logger.critical(f"Фатальная ошибка при парсинге YAML {file_path}:\n%s", e, exc_info=True)
            raise e
    if data:
        logger.debug(f"Загружены данные из YAML {file_path}")
    else:
        logger.debug(f"Пустой словарь из YAML {file_path}")
    return data

def save_yaml(filename:str, path:str, data:dict) -> str:
    """
    Сохраняет словарь в YAML-файл.

    Использует pyyaml для сериализации данных в читаемый формат.
    Отключает сортировку ключей и использует блочный стиль.

    :param filename: Имя файла без расширения (например, 'config').
    :type filename: str
    :param path: Путь к директории, где будет сохранён файл (должен включать '/' в конце).
    :type path: str
    :param data: Словарь с данными для сохранения.
    :type data: dict
    :return: Полный путь к созданному YAML-файлу.
    :rtype: str
    """
    # Полный путь к файлу
    file_path = f'{path}{filename}.yaml'

    # Записываем данные в YAML-файл
    with open(file_path, 'w') as yaml_file:
        dump(data, yaml_file, default_flow_style=False, sort_keys=False)
    return file_path