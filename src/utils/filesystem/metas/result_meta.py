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
    Ключ: sample_name + fingerprint.
    Коллекция: 'results'
    """
    name: str  # Имя образца
    fingerprint: str  # fingerprint образца
    short_id: str  # Идентификатор образца
    # Динамические результаты по пайплайнам
    # Формат: {"pipeline_name": {"metric": value, "outputs": [...], "status": "ok", ...}}
    results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Дата последнего обновления
    modified: Optional[datetime] = field(default=None)
    # Внутренний хэш для контроля целостности
    _fingerprint: hashlib_blake2s = field(default_factory=hashlib_blake2s)

    @staticmethod
    def from_db(doc: Dict[str, Any]) -> 'ResultMeta':
        """
        Восстанавливает объект из документа БД.
        """
        meta = ResultMeta(
            name=doc.get("name", ""),
            fingerprint=doc.get("fingerprint", ""),
            results=doc.get("results", {}),
            modified=doc.get("modified")
        )
        # Восстанавливаем хэш
        for pipeline in meta.results:
            meta._update_fingerprint(pipeline)
        return meta

    def set_result(
                   self,
                   pipeline: str,
                   data: Dict[str, Any],
                   status: str = "ok"
                  ) -> None:
        """
        Добавляет или обновляет результаты пайплайна.
        """
        if pipeline not in self.results:
            self.results[pipeline] = {}

        self.results[pipeline].update(data)
        self.results[pipeline]['status'] = status
        self.results[pipeline]['updated_at'] = datetime.now()

        self._update_fingerprint(pipeline)
        self.modified = datetime.now()

    def get_result(self, pipeline: str, key: str) -> Any:
        """
        Возвращает значение метрики пайплайна.
        """
        return self.results.get(pipeline, {}).get(key)

    def has_result(self, pipeline: str, key: str) -> bool:
        """
        Проверяет, существует ли результат.
        """
        return self.results.get(pipeline, {}).get(key) is not None

    def is_pipeline_complete(self, pipeline: str) -> bool:
        """
        Проверяет, завершён ли пайплайн успешно.
        """
        return self.results.get(pipeline, {}).get('status') == 'ok'

    def get_output_paths(
                         self,
                         pipeline: str,
                         file_type: str
                        ) -> Dict[str, Set[Path]]:
        """
        Возвращает множество путей выходных файлов.
        """
        outputs = self.results.get(pipeline, {}).get('outputs', [])
        return {Path(p) for p in outputs if p}

    def _update_fingerprint(self, pipeline: str) -> None:
        """
        Обновляет внутренний хэш на основе результата.
        """
        h = hashlib_blake2s()
        for k, v in self.results[pipeline].items():
            h.update(f"{k}={v}".encode())
            h.update(b'|')
        self._fingerprint.update(h.digest())
        self._fingerprint.update(b'|')

    def finalize(self) -> str:
        """
        Формирует финальный fingerprint результатов.
        """
        return self._fingerprint.hexdigest()
