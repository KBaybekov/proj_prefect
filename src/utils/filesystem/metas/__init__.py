# -*- coding: utf-8 -*-

from __future__ import annotations
from utils.logger import get_logger
from datetime import datetime
from hashlib import blake2s as hashlib_blake2s
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = get_logger(name=__name__)


def update_fingerprint(
                       main_fingerprint:hashlib_blake2s,
                       fingerprint2add:str
                       ) -> hashlib_blake2s:
    """
    Обновляет отпечаток объекта, добавляя строку другого отпечатка и разделитель
    """
    main_fingerprint.update(fingerprint2add.encode())
    main_fingerprint.update(b'|')
    return main_fingerprint

def generate_final_fingerprint(raw_fingerprint: hashlib_blake2s) -> str:
    """
    Создаёт строку отпечатка объекта на основе хэша
    """
    return raw_fingerprint.hexdigest()

def max_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    return b if a is None or (b and b > a) else a

def min_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    return b if a is None or (b and b < a) else a

from .batch_meta import BatchMeta
from .sample_meta import SampleMeta

def update_file_in_meta(
                        meta_obj: Union[BatchMeta, SampleMeta],
                        file_path: Path,
                        diff: Dict[str, List[Any]]
                       ) -> bool:
    """
    Обновляет структуры в BatchMeta/SampleMeta при изменении файла.
    Возвращает True, если какие-то метаданные были изменены.
    
    :param meta_obj: Объект BatchMeta или SampleMeta.
    :param file_id: Путь к изменённому файлу.
    :param diff: Словарь изменений {size: [old, new], modified: [old, new]}.
    :return: None
    """
    return meta_obj.edit_file_meta({file_path: diff})

