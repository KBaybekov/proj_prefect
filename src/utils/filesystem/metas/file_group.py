# -*- coding: utf-8 -*-
"""
Хранение группы файлов и их общего размера
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Set
from utils.logger import get_logger

logger = get_logger(name=__name__)

@dataclass(slots=True)
class FileGroup:
    """Подгруппа файлов (pass/fail): список симлинков и суммарный размер."""
    files: Set[Path] = field(default_factory=set)
    size: int = 0

    def add2file_group(self, file: Path, size: int) -> bool:
        """
        Добавляет файл в группу файлов, если он ещё не был добавлен.
        Возвращает True, если файл был добавлен
        """
        file_added: bool = False
        if file not in self.files:
            self.files.add(file)
            old_size = self.size
            self.size += size
            logger.debug(f"Добавлен файл: {file}. Размер FileGroup изменён: {old_size} -> {self.size}")
            file_added = True
        else:
            logger.debug(f"Файл {file} уже был добавлен в FileGroup")
        return file_added

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        """
        Проходит по словарю изменений и обновляет размеры файлов в группе.
        Возвращает True, если размеры были изменены
        """
        size_changed: bool = False
        # проходим по словарям изменений
        for mod_data in edit_dict.values():
            size_diffs = mod_data.get('size')
            if size_diffs:
                diff = size_diffs[1] - size_diffs[0]
                if diff:
                    old_size = self.size
                    self.size += diff
                    logger.debug(f"Размер FileGroup изменён: {old_size} -> {self.size}")
                    size_changed = True
        return size_changed
        
    def remove_file_from_group(self, file:Path, size:int) -> bool:
        """
        Удаляет файл из группы файлов, если он был добавлен.
        Возвращает True, если файл был удалён
        """
        file_removed: bool = False
        if file in self.files:
            self.files.remove(file)
            old_size = self.size
            self.size -= size
            logger.debug(f"Удалён файл: {file}. Размер FileGroup изменён: {old_size} -> {self.size}")
            file_removed = True
        return file_removed
