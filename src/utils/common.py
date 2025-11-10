import argparse
import os
import subprocess
import time
from datetime import datetime
from typing import Optional
from yaml import dump, safe_load, YAMLError
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(name=__name__)

def setup_parser() -> argparse.ArgumentParser:
    """
    Создает и настраивает парсер аргументов командной строки.
    """
    parser = argparse.ArgumentParser(
                                     description="FsCrawler - инструмент для сканирования файловой системы и работы с метаданными",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
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


def env_var(var:str) -> str:
    """
    Возвращает значение переменной, указанной в .env-файле
    ----------
    :param var: Имя переменной
    :return: Значение переменной
    """
    val = os.getenv(var)
    if val == None:
        print(f"Env var {var} not set")
    return str(val)


def run_shell_cmd(cmd:str, timeout:int=0, debug:str='') -> dict:
    """
    Выполняет shell-команду с таймаутом и логированием результатов.

    :param cmd: Команда для выполнения (строка, которая будет передана в shell)
    :param timeout: Максимальное время выполнения в секундах (0 - без таймаута)
    :param debug: Уровень логирования ('errors' - только ошибки, 'info' - stdout, 'all' - всё)
    :returns: Словарь с результатами выполнения:
        - log: метаданные выполнения (статус, временные отметки, код возврата)
        - stderr: содержимое stderr (если есть)
        - stdout: содержимое stdout (если есть)
    """

    timeout_param = None if timeout == 0 else timeout
    # Время начала (общее)
    start_time = time.time()
    cpu_start_time = time.process_time()
    start_datetime = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    stdout, stderr = "", ""

    result = subprocess.Popen(args=cmd,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              universal_newlines=True,
                              executable="/bin/bash",
                              bufsize=1,
                              cwd=None,
                              env=None)
    
    try:       
        # Ожидаем завершения с таймаутом
        stdout, stderr = result.communicate(timeout=timeout_param)
        # Построчно читаем стандартный вывод и ошибки в зависимости от уровня дебага
        """
        if debug:
            streams = []
            if debug in ['errors', 'all']:
                streams.append(('STDERR', stderr.splitlines()))
            if debug in ['info', 'all']:
                streams.append(('STDOUT', stdout.splitlines()))

            for label, stream in streams:
                for line in stream:
                    print(f"{label}: {line.strip()}")
        """            
        if debug:
            if debug in ['errors', 'all'] and stderr:
                for line in stderr.splitlines():
                    logger.error(f"STDERR: {line.strip()}")
            if debug in ['info', 'all'] and stdout:
                for line in stdout.splitlines():
                    logger.info(f"STDOUT: {line.strip()}")

        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)

        if result.returncode == 0:
            logger.info(f"Команда '{cmd}' выполнена успешно.")
        else:
            logger.warning(f"Команда:\n '{cmd}' \nзавершилась с ошибкой (код {result.returncode}).")

        # Лог выполнения случае завершения без исключений
        return {
            'log': {
                'status': 'OK' if result.returncode == 0 else 'FAIL',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': result.returncode
            },
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''
        }

    except subprocess.TimeoutExpired:
        result.kill()
        stdout, stderr = result.communicate()
        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)
        # Лог при тайм-ауте
        logger.error(f"Команда:\n '{cmd}' \nпревысила таймаут ({timeout}s).")
        return {
            'log': {
                'status': 'FAIL',
                'start_time': start_datetime,
                'end_time': end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': "TIMEOUT"
            },
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''
        }
    # Прерывание пользователем
    except KeyboardInterrupt:
        result.kill()
        print('INTERRUPTED')
        duration_sec, duration, cpu_duration, end_datetime = get_duration(start_time=start_time, cpu_start_time=cpu_start_time)
        logger.error(f"Команда:\n '{cmd}' \nпрервана пользователем (KeyboardInterrupt).")
        return {
            'log':
                {'status': 'FAIL',
                'start_time':start_datetime,
                'end_time':end_datetime,
                'duration': duration,
                'duration_sec': duration_sec,
                'cpu_duration_sec': round(cpu_duration, 2),
                'exit_code': 'INTERRUPTED'},
            'stderr': stderr.strip() if stderr else '',
            'stdout': stdout.strip() if stdout else ''   
                }


def get_duration(duration_sec:float=0, start_time:float=0, cpu_start_time:float=0, precision:str='s') -> tuple:
    """
    Возвращает продолжительность выполнения команды в необходимом формате.

    :param duration_sec: Предварительно рассчитанная длительность в секундах (если 0 - вычисляется)
    :param start_time: Время начала выполнения (timestamp)
    :param cpu_start_time: CPU-время начала выполнения (process_time)
    :param precision: Точность форматирования ('d'-дни, 'h'-часы, 'm'-минуты, 's'-секунды)
    :returns: Кортеж с:
        - duration_sec: общая длительность в секундах
        - time_str: отформатированная строка времени
        - cpu_duration: CPU-время выполнения
        - end_datetime: строка с датой/временем завершения
    """

    # Время завершения (общее)
    duration_sec = int(time.time() - start_time)
    
    # Форматируем секунды в дни, часы и минуты
    time_str = convert_secs_to_dhms(secs=duration_sec, precision=precision)

    cpu_duration = time.process_time() - cpu_start_time
    end_datetime = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    return (duration_sec, time_str, cpu_duration, end_datetime)

def convert_secs_to_dhms(secs:int, precision:str='s') -> str:
    """
    Конвертирует секунды в удобочитаемый формат (дни/часы/минуты/секунды).

    :param secs: Количество секунд для конвертации
    :param precision: Максимальная единица точности ('d','h','m','s')
    :returns: Отформатированная строка времени (например "1h 30m")
    :raises ValueError: Если указана недопустимая точность
    """

    # Форматируем секунды в дни, часы и минуты
    if precision not in ['d', 'h', 'm', 's']:
        raise ValueError("Неправильное указание уровня точности!")
    d, not_d = divmod(secs, 86400) # Возвращает кортеж из целого частного и остатка деления первого числа на второе
    h, not_h = divmod(not_d, 3600)
    m, s = divmod(not_h, 60)
    measures = {'d':d, 'h':h, 'm':m, 's':s}
    to_string = []
    # Разряд времени пойдет в результат, если его значение не 0
    for measure, val in measures.items():
        if val !=0:
            to_string.append(f'{val}{measure}')
        if measure == precision:
            break
    # Формируем строку и определяем уровни точности
    time_str = " ".join(to_string)
    if len(time_str) == 0:
        time_str = (f'< 1{precision}')
    return time_str

def max_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    return b if a is None or (b and b > a) else a

def min_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    return b if a is None or (b and b < a) else a

def load_yaml(
              file_path:Path,
              encoding:str = "utf-8",
              critical:bool = False,
              subsection:str = ''
             ) -> dict:
    """
    Универсальная функция для загрузки данных из YAML-файла.

    :param file_path: Путь к YAML-файлу. Ожидается объект Path, указывающий на местоположение файла с данными.
    :param encoding: Тип кодировки ("utf-8")
    :param critical: Возвращает ошибку, если файл не найден
    :param subsection: Опциональный параметр. Если передан, функция вернёт только данные из указанной
                       секции (например, конкретного этапа пайплайна). Если пусто, возвращаются все данные.
                       По умолчанию - пустая строка, что означает возврат всего содержимого файла.
    
    :return: Возвращает словарь с данными из YAML-файла. Если указан параметр subsection и он присутствует
             в YAML, возвращается соответствующая секция, иначе — всё содержимое файла.
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
    Сохраняет словарь в файл в формате YAML. Возвращает полный путь нового файла.
    
    :param filename: Имя файла для сохранения (например, 'config.yaml')
    :param path: Путь к директории, где будет сохранён файл
    :param data: Словарь с данными, которые нужно сохранить в YAML
    :return file_path: Полный путь нового YAML
    """
    # Полный путь к файлу
    file_path = f'{path}{filename}.yaml'

    # Записываем данные в YAML-файл
    with open(file_path, 'w') as yaml_file:
        dump(data, yaml_file, default_flow_style=False, sort_keys=False)
    return file_path
