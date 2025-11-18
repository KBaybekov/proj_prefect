# -*- coding: utf-8 -*-
"""
Хранение нескольких видов файлов и некоторых метаданных этих групп
"""
from .file_subset import FileSubset
from utils.logger import get_logger
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

logger = get_logger(name=__name__)

@dataclass(slots=True)
class FileSet:
    """
    Набор файлов, относящийся к одному объекту (батчу/образцу), включает:
        • fast5 - набор файлов fast5
        • pod5 - набор файлов pod5
        • fq - набор файлов fq
        • size - общий размер файлов
    """
    size_total: int = 0
    size_pass: int = 0
    size_fail: int = 0
    # Счётчик для размера нормальных fast5/pod5
    size_pass_sourcefiles: int = 0
    # Сохраняем файл и его отпечаток
    files: Dict[Path, str] = field(default_factory=dict)
    fast5: FileSubset = field(default_factory=FileSubset)
    pod5: FileSubset = field(default_factory=FileSubset)
    fq: FileSubset = field(default_factory=FileSubset)

    @staticmethod
    def from_db(doc: Dict[str, Any]) -> 'FileSet':
        """
        Восстанавливает объект FileSet из документа БД.
        """
        return FileSet(
                        size_total=doc.get("size_total", 0),
                        size_pass=doc.get("size_pass", 0),
                        size_fail=doc.get("size_fail", 0),
                        size_pass_sourcefiles=doc.get("size_pass_sourcefiles", 0),
                        files={Path(k): v for k, v in doc.get("files", {}).items()},
                        fast5=FileSubset.from_db(doc.get("fast5", {})),
                        pod5=FileSubset.from_db(doc.get("pod5", {})),
                        fq=FileSubset.from_db(doc.get("fq", {}))
                        )

    def _file_added_to_fileset(
                               self,
                               file: Path,
                               fingerprint:str,
                               extension: str,
                               size: int,
                               qc_pass: bool
                              ) -> bool:
        if file in self.files:
            return False
        else:
            file_added = self.add2file_set(
                                           file,
                                           fingerprint,
                                           extension,
                                           size,
                                           qc_pass
                                          )
            return file_added

    def add2file_set(self, file: Path, fingerprint:str, extension: str, size: int, qc_pass: bool) -> bool:
        """
        Добавляет файл в сет файлов (в зависимости от расширения) 
        и соответствующий FileSubset, если он ещё не был добавлен.
        Возвращает True, если файл был добавлен
        """
        file_added: bool = False

        if file not in self.files:
            # выбираем сабсет на основе расширения файла
            if "fast5" in extension:
                logger.debug(f"Добавляем файл {file} в FileSubset 'fast5'")
                file_added = self.fast5.add2file_subset(file, size, qc_pass)
            elif "pod5" in extension:
                logger.debug(f"Добавляем файл {file} в FileSubset 'pod5'")
                file_added = self.pod5.add2file_subset(file, size, qc_pass)
            elif any([
                    fq_ext in extension
                    for fq_ext in ['fq', 'fastq']
                    ]):
                logger.debug(f"Добавляем файл {file} в FileSubset 'fq'")
                file_added = self.fq.add2file_subset(file, size, qc_pass)
            else:
                logger.debug(f"Неизвестный тип файла: {extension}; файл не добавлен в FileSubset")
                return False

            if file_added:
                self.files[file] = fingerprint
                # Добавляем размер файла к общему размеру
                self.size_total += size
                if qc_pass:
                    self.size_pass += size
                    if extension in ['fast5', 'pod5']:
                        self.size_pass_sourcefiles += size
                else:
                    self.size_fail += size
        return file_added

    def edit_file_meta(self, edit_dict: Dict[Path, Dict[str, List[Any]]]) -> bool:
        """
        Функция для обновления метаданных файлов в FileSet
        Возвращает True, если были изменены метаданные
        """
        fileset_changed: bool = False
        # проходим по словарям изменений
        for file, mod_data in edit_dict.items():
            size_diffs = mod_data.get('size')
            if size_diffs:
                diff = size_diffs[1] - size_diffs[0]
                if diff:
                    old_total = self.size_total
                    self.size_total += diff
                    logger.debug(f"Общий размер FileSet изменён: {old_total} -> {self.size_total}")
                    fileset_changed = True
                    if 'pass' in file.name:
                        old_pass = self.size_pass
                        self.size_pass += diff
                        logger.debug(f"Общий размер файлов 'pass' изменён: {old_pass} -> {self.size_pass}")
                        if file.suffix in ['.fast5', '.pod5']:
                            old_sourcefiles = self.size_pass_sourcefiles
                            self.size_pass_sourcefiles += diff
                            logger.debug(f"Общий размер исходных файлов изменён: {old_sourcefiles} -> {self.size_pass_sourcefiles}")
                    else:
                        old_fail = self.size_fail
                        self.size_fail += diff
                        logger.debug(f"Общий размер файлов 'fail' изменён: {old_fail} -> {self.size_fail}")
        return fileset_changed

    def remove_from_fileset(self, file:Path, extension:str, size:int, qc_pass:bool) -> bool:
        """
        Удаляет файл из сета файлов (в зависимости от расширения) 
        и соответствующего FileSubset.
        Возвращает True, если файл был удалён
        """
        file_removed: bool = False
        # выбираем сабсет на основе расширения файла
        if "fast5" in extension:
            file_removed = self.fast5.remove_file_from_subset(file, size, qc_pass)
            if file_removed:
                logger.debug(f"Удалён файл {file} из FileSubset 'fast5'")
        elif "pod5" in extension:
            file_removed = self.pod5.remove_file_from_subset(file, size, qc_pass)
            if file_removed:
                logger.debug(f"Удалён файл {file} из FileSubset 'pod5'")
        elif any([
                fq_ext in extension
                for fq_ext in ['fq', 'fastq']
                ]):
            file_removed = self.fq.remove_file_from_subset(file, size, qc_pass)
            if file_removed:
                logger.debug(f"Удалён файл {file} из FileSubset 'fq'")

        if file_removed:
            del self.files[file]
            # Удаляем размер файла из общего размера
            self.size_total -= size
            if qc_pass:
                self.size_pass -= size
                if extension in ['fast5', 'pod5']:
                    self.size_pass_sourcefiles -= size
            else:
                    self.size_fail -= size
        return file_removed
