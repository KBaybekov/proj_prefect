# -*- coding: utf-8 -*-
"""
Хранение нескольких групп файлов и их общего размера
"""
from .file_group import FileGroup
from utils.logger import get_logger
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

logger = get_logger(name=__name__)

@dataclass(slots=True)
class FileSubset:
    """
    Набор файлов одного типа (напр., fast5 или pod5):
        • size  — общий размер (pass + fail)
        • pass_ — группа с файлами, прошедшими QC
        • fail  — группа с файлами, не прошедшими QC
    """
    size: int = 0
    pass_: FileGroup = field(default_factory=FileGroup)
    fail:  FileGroup = field(default_factory=FileGroup)

    @staticmethod
    def from_db(doc: Dict[str, Any]) -> 'FileSubset':
        """
        Восстанавливает объект FileSubset из документа БД.
        """
        return FileSubset(
                          size=doc.get("size", 0),
                          pass_=FileGroup(
                                          files={Path(f) for f in doc.get("pass", {}).get("files", set())},
                                          size=doc.get("pass", {}).get("size", 0)
                                         ),
                          fail=FileGroup(
                                         files={Path(f) for f in doc.get("fail", {}).get("files", set())},
                                         size=doc.get("fail", {}).get("size", 0)
                                        ),
                         )


    def add2file_subset(self, file: Path, size: int, qc_pass: bool) -> bool:
        """
        Добавляет файл в сабсет файлов (в зависимости от флага QC), если он ещё не был добавлен.
        Возвращает True, если файл был добавлен
        """
        file_added: bool = False
        if qc_pass:
            logger.debug(f"Добавляем файл {file} в FileSubset 'pass_'")
            file_added = self.pass_.add2file_group(file, size)
        else:
            logger.debug(f"Добавляем файл {file} в FileSubset 'fail'")
            file_added = self.fail.add2file_group(file, size)
        if file_added:
            self.size += size
        return file_added

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        subset_changed: bool = False
        if not edit_dict:
            logger.debug("Изменений в метаданных нет")
            return subset_changed
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            size_diffs = mod_data.get('size')
            if size_diffs:
                diff = size_diffs[1] - size_diffs[0]
                if diff:
                    old_size = self.size
                    self.size += diff
                    logger.debug(f"Размер FileSubset изменён: {old_size} -> {self.size}")
                    subset_changed = True
        return subset_changed

    def remove_file_from_subset(self, file:Path, size:int, qc_pass:bool) -> bool:
        """
        Удаляет файл из сабсета файлов, если он был добавлен.
        Возвращает True, если файл был удалён
        """
        file_removed: bool = False
        if qc_pass:
            logger.debug(f"Удаляем файл {file} из FileSubset 'pass_'")
            file_removed = self.pass_.remove_file_from_group(file, size)
        else:
            logger.debug(f"Удаляем файл {file} из FileSubset 'fail'")
            file_removed = self.fail.remove_file_from_group(file, size)
        if file_removed:
            self.size -= size
        return file_removed
