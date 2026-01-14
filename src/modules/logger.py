# -*- coding: utf-8 -*-
"""
Логгирование работы в файл (debug-уровень) и в stdout (info-уровень)
"""
from __future__ import annotations
from logging import getLogger, Formatter, FileHandler, StreamHandler, INFO, DEBUG
from pathlib import Path
from yaml import safe_load

log_file = safe_load(Path('/common_share/github/proj_prefect/config/config.yaml').read_text())['log']

fmt = Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
    )


def get_file_handler():
    file_handler = FileHandler(Path(log_file)) # type: ignore
    file_handler.setLevel(DEBUG)
    file_handler.setFormatter(fmt)
    return file_handler

def get_stream_handler():
    stream_handler = StreamHandler()
    stream_handler.setLevel(INFO)
    stream_handler.setFormatter(fmt)
    return stream_handler

def get_logger(name:str):
    logger = getLogger(name)
    logger.setLevel(DEBUG)
    logger.addHandler(get_file_handler())
    logger.addHandler(get_stream_handler())
    return logger
