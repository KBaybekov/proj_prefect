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

from .file_group import FileGroup
from .file_subset import FileSubset
from .file_set import FileSet
from .source_file_meta import SourceFileMeta
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
    def _find_containing_structures(
                                    meta_obj: Union[BatchMeta, SampleMeta],
                                    file_path: Path
                                   ) -> List[BatchMeta|
                                             SampleMeta|
                                             FileSet|
                                             FileSubset|
                                             FileGroup]:
        """
        Находит все структуры в объекте, где присутствует файл.
        """
        subset:FileSubset
        filegroup:FileGroup
        structures:List[
                        BatchMeta|
                        SampleMeta|
                        FileSet|
                        FileSubset|
                        FileGroup
                       ]
        
        # добавляем сам объект в список структур
        structures = [meta_obj]
        # добавляем сам FileSet, проходим по FileSet, собирая все структуры, где файл присутствует
        structures.append(meta_obj.files) # type: ignore
        for filetype in ["fast5", "pod5", "fq"]:
            subset = getattr(meta_obj.files, filetype) # type: ignore
            for attr_name in ["pass_", "fail"]:
                filegroup = getattr(subset, attr_name)
                if file_path in filegroup.files:
                    structures.append(subset)
                    structures.append(subset.__getattribute__(attr_name))
        return structures

    # Находим все структуры, где файл присутствует
    containing_structures = _find_containing_structures(meta_obj, file_path) # type: ignore
    # обновляем их
    meta_changed = []
    for structure in containing_structures:
        meta_changed.append(structure.edit_file_meta({file_path: diff}))
        if meta_changed[-1]:
            logger.debug(f"Изменена структура: {type(structure)}")
    return any([f for f in meta_changed])
