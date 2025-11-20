# encoding: utf-8
"""
Модуль для загрузки функций из data_shaper_*.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Tuple, Any, Dict
import importlib.util
import sys
from utils.logger import get_logger

logger = get_logger(__name__)


# Типы для аннотаций

# На вход подаются: мета образца, мета результатов образца, шаблон команды Nextflow, задание, сервисные данные
# На выходе - CLI команда Nextflow, путь к TSV с входными данными
IDataFunc = Callable[
                     [str, Any, Dict[str, Any]],
                     Tuple[str, Path]
                    ]
# на вход подаются: папка результатов, папка с QC, папка с логами
# На выходе - словарь вида {коллекция: {вид данных: данные}}
ODataFunc = Callable[
                     [Path, Path, Path],
                     Dict[str, Dict[str, Any]]
                    ]
InputDataFunc = IDataFunc
OutputDataFunc = ODataFunc


def load_shaper_functions(
                          shaper_path: Path
                         ) -> Tuple[InputDataFunc, OutputDataFunc]:
    """
    Динамически загружает функции generate_pipeline_input_data и generate_pipeline_output_data
    из указанного .py-файла.

    :param shaper_path: Путь к файлу data_shaper
    :return: Кортеж (input_func, output_func)
    :raises ImportError: Если файл не найден или функции отсутствуют
    """
    def _check_if_not_exist(
                            module: Any,
                            func_name:str
                           ) -> None:
        """
        Проверяет, что функция существует в модуле.
        """
        if not hasattr(module, func_name):
            logger.error(f"Function '{func_name}' not found in {shaper_path}")
            raise AttributeError(f"Function '{func_name}' not found in {shaper_path}")
    
    def _check_if_not_func(
                           func_name: str,
                           func: Any
                          ) -> None:
        """
        Проверяет, что объект является функцией Python.
        """
        if not callable(func):
            logger.error(f"{func_name} is not callable in {shaper_path}")
            raise TypeError(f"{func_name} is not callable in {shaper_path}")

    if not shaper_path.exists():
        logger.error(f"Data shaper not found: {shaper_path}")
        raise FileNotFoundError(f"Data shaper not found: {shaper_path}")

    module_name = f"{shaper_path.stem}"  # Уникальное имя модуля
    logger.debug(f"Начало импорта модуля {module_name}")

    # Проверяем sys.modules, чтобы избежать повторной загрузки
    if module_name in sys.modules:
        logger.debug(f"Модуль найден в sys.modules: {module_name}. Импорт...")
        module = sys.modules[module_name]
    
    else:
        logger.debug(f"Начало загрузки модуля {module_name}")
        spec = importlib.util.spec_from_file_location(module_name, shaper_path)
        if spec is None or spec.loader is None:
            logger.error(f"Cannot load spec from: {shaper_path}")
            raise ImportError(f"Cannot load spec from {shaper_path}")
        logger.debug(f"Загружен ModuleSpec для {module_name}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
            logger.debug(f"Модуль загружен: {module_name}")
        except Exception as e:
            logger.error(f"Модуль НЕ загружен: {module_name}")
            raise ImportError(f"Failed to load shaper {shaper_path}: {e}") from e
    
    logger.debug(f"Успешный импорт {module_name}")
    
    # Проверяем существование нужных функций в модуле
    for name in [
                 "generate_pipeline_input_data",
                 "generate_pipeline_output_data"
                ]:
        _check_if_not_exist(module, name)   

    # Проверяем, что функции действительно являются функциями
    input_func = getattr(module, "generate_pipeline_input_data")
    output_func = getattr(module, "generate_pipeline_output_data")
    for name, func  in {
                        "generate_pipeline_input_data":input_func,
                        "generate_pipeline_output_data":output_func
                       }:
        _check_if_not_func(name, func)
    
    logger.debug(f"Успешный импорт функций из {module_name}")
    return input_func, output_func
