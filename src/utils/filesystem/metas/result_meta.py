# src/utils/filesystem/metas/result_meta.py
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import blake2s as hashlib_blake2s
from pathlib import Path
from typing import Any, Dict, Optional, Set
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class ResultMeta:
    """
    Динамический класс для хранения результатов выполнения пайплайнов.
    Ключ: sample_id.
    Коллекция: 'results'
    """
    name: str  # Имя образца
    fingerprint: str  # fingerprint образца
    sample_id: str  # Идентификатор образца
    # Динамические результаты по пайплайнам
    # Формат: {"pipeline_id": {"metric": value, "outputs": [...], "status": "ok", ...}}
    results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Дата создания
    created: Optional[datetime] = field(default=None)
    # Дата последнего обновления
    modified: Optional[datetime] = field(default=None)

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any]
                 ) -> 'ResultMeta':
        """
        Восстанавливает объект из документа БД.
        """
        return ResultMeta(
                          name=doc.get("name", ""),
                          fingerprint=doc.get("fingerprint", ""),
                          sample_id=doc.get("sample_id", ""),
                          results=doc.get("results", {}),
                          created=doc.get("created"),
                          modified=doc.get("modified")
                         )
    
    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Конвертирует объект SourceFileMeta в словарь.
        """
        dict_obj = self.__dict__
        return dict_obj
