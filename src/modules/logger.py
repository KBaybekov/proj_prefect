
# -*- coding: utf-8 -*-
"""
Логгирование работы в файл (debug-уровень) и в stdout (info-уровень)
"""
from __future__ import annotations
from logging import getLogger, Formatter, FileHandler, StreamHandler, INFO, DEBUG
from pathlib import Path
from csv import writer as csv_writer, QUOTE_ALL
from io import StringIO
from datetime import datetime
from yaml import safe_load

now = datetime.now()
formatted_time = now.strftime("%d-%m-%Y_%H:%M:%S")

log_file = Path(safe_load(Path('/common_share/github/proj_prefect/config/config.yaml').read_text())['log'])
log_file.parent.mkdir(exist_ok=True)

# Список колонок для заголовка
CSV_COLUMNS = ["Day", "Month", "Year", "Hour", "Minutes", "Seconds", "Microseconds", "Level", "Logger", "Location", "Message"]


class CsvFormatter(Formatter):
    def __init__(self):
        super().__init__()
        self.output = StringIO()
        self.writer = csv_writer(self.output, quoting=QUOTE_ALL)

    def format(self, record):
        dt = datetime.fromtimestamp(record.created)
        self.writer.writerow([
            dt.strftime("%d"),       # Day
            dt.strftime("%m"),       # Month
            dt.strftime("%Y"),       # Year
            dt.strftime("%H"),       # Hour
            dt.strftime("%M"),       # Minutes
            dt.strftime("%S"),       # Seconds
            dt.strftime("%f"),       # Microseconds
            record.levelname,
            record.name,
            f"{record.funcName}:{record.lineno}",
            record.msg
        ])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()

# Console format
console_fmt = Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
    )

def get_file_handler():
    # Если файл новый, записываем заголовок
    if not log_file.exists():
        with open(log_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv_writer(f, quoting=QUOTE_ALL)
            writer.writerow(CSV_COLUMNS)
    file_handler = FileHandler(Path(log_file), encoding='utf-8') 
    file_handler.setLevel(DEBUG)
    file_handler.setFormatter(CsvFormatter())
    return file_handler

def get_stream_handler():
    stream_handler = StreamHandler()
    stream_handler.setLevel(INFO)
    stream_handler.setFormatter(console_fmt)
    return stream_handler

def get_logger(name:str):
    logger = getLogger(name)
    # Чтобы избежать дублирования логов при повторных вызовах get_logger
    if not logger.handlers:
        logger.setLevel(DEBUG)
        logger.addHandler(get_file_handler())
        logger.addHandler(get_stream_handler())
    return logger
