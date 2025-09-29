# -*- coding: utf-8 -*-
"""
MongoDB helpers
"""

from __future__ import annotations

import pymongo
from pymongo import MongoClient

from typing import Any, Dict, List, Mapping, Optional
from datetime import datetime, timezone

from enum import Enum
from dataclasses import dataclass, field, fields, asdict, is_dataclass
from pathlib import Path
import yaml
from utils.logger import get_logger

logger = get_logger(name=__name__)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize(value: Any) -> Any:
    """Рекурсивная нормализация значений для BSON:
    - dataclass → dict (с дальнейшей рекурсией)
    - dict/list/tuple/set → рекурсивно нормализуем элементы
    - Path → POSIX-строка
    - datetime → UTC (через _to_utc)
    Прочие типы возвращаются как есть.
    """
    if is_dataclass(value):
        # Преобразуем dataclass в словарь и продолжаем рекурсивную нормализацию
        return _normalize(asdict(value)) # type: ignore

    if isinstance(value, Path):
        return value.as_posix()

    if isinstance(value, datetime):
        return _to_utc(value)

    if isinstance(value, dict):
        # Рекурсивно нормализуем каждое значение словаря
        return {k: _normalize(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        # Рекурсивно нормализуем элементы коллекций (возвращаем список)
        return [_normalize(v) for v in value]
    return value



def _dir_from_str(s: str) -> int:
    s = s.strip().upper()
    if s == "ASC":
        return pymongo.ASCENDING
    if s == "DESC":
        return pymongo.DESCENDING
    raise ValueError(f"Unknown index direction: {s}")


def to_mongo(obj: Any, *, keep_empty: bool = True) -> Any:
    """Рекурсивно превращает датаклассы/сложные объекты в JSON-совместимые структуры для PyMongo.
    - Сохраняет пустые dict/list, если keep_empty=True.
    - Преобразует Path→str, set→list, Enum→.value.
    """
    # dataclass → dict (только публичные поля)
    if is_dataclass(obj):
        out = {}
        for f in fields(obj):
            name = f.name
            if name.startswith("_"):
                continue
            out[name] = to_mongo(getattr(obj, name), keep_empty=keep_empty)
        return out

    # словари
    if isinstance(obj, Mapping):
        out = {str(k): to_mongo(v, keep_empty=keep_empty) for k, v in obj.items()}
        # ничего не выбрасываем, даже если пусто, кроме явно None по желанию
        return out

    # коллекции (кроме строк и bytes)
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_mongo(v, keep_empty=keep_empty) for v in obj]

    # простые типы и спец-случаи
    if isinstance(obj, Path):
        return obj.as_posix()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (str, int, float, bool, type(None), datetime)):
        return obj

    # запасной вариант — строковое представление (лучше не падать)
    return str(obj)



@dataclass  
class ConfigurableMongoDAO:
    """Универсальный DAO с динамическими коллекциями и ensure_indexes()."""
    client: MongoClient
    db_name: str
    collections_config: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    db: pymongo.database.Database = field(init=False) # type: ignore

    def __post_init__(self) -> None:
        self.db = self.client[self.db_name]
        # Динамически добавляем атрибуты-коллекции
        for coll_name in self.collections_config.keys():
            setattr(self, coll_name, self.db.get_collection(coll_name))
        # Создаём индексы
        if self.collections_config:
            logger.debug(f"Проверяем индексы в {self.collections_config.keys()}")
            self.ensure_indexes()



    def ensure_indexes(self) -> None:
        """Идёмпотентно создаём индексы по конфигу YAML."""
        for coll_name, cfg in self.collections_config.items():
            coll = getattr(self, coll_name, None)
            if coll is None:
                continue
            for spec in cfg.get("indexes", []):
                keys = spec.get("keys", [])
                name = spec.get("name")
                kwargs = {k: v for k, v in spec.items() if k not in {"keys", "name"}}
                if name:
                    kwargs["name"] = name
                coll.create_index(keys, **kwargs)


    def upsert(self, collection: str, key: Dict[str, Any], doc: Dict[str, Any]) -> None:
        coll: pymongo.collection.Collection # type: ignore

        coll = getattr(self, collection)
        d = _normalize(doc)
        #d = to_mongo(doc)
        now = datetime.now(timezone.utc)
        d.setdefault("updated_at_DB", now)
        coll.update_one(key, {"$set": d, "$setOnInsert": {"created_at_DB": now}}, upsert=True)


    def update_one(self, collection: str, key: Dict[str, Any], doc: Dict[str, Any]) -> None:
        """
        Обновляет существующий документ в указанной коллекции.
        
        Args:
            collection: Название коллекции.
            key: Фильтр для поиска документа (аналог MongoDB query).
            doc: Данные для обновления (аналог MongoDB $set).
        """
        coll: pymongo.collection.Collection # type: ignore
        
        coll = getattr(self, collection)
        d = _normalize(doc)  # Рекурсивная нормализация данных
        now = datetime.now(timezone.utc)
        
        # Добавляем/обновляем временную метку последнего изменения
        d["updated_at_DB"] = now
        
        # Выполняем обновление первого совпадающего документа
        coll.update_one(key, {"$set": d})
        logger.debug(f"Обновлено в {collection}: {key} -> {doc}")


    def find(self,
             collection: str,
             query: Dict[str, Any],
             projection: Optional[Dict[str, int]] = None,
             limit: int = 0
            ) -> List[Dict[str, Any]]:
        """
        Ищет документы в указанной коллекции.
        :param collection: Название коллекции.
        :param query: Фильтр для поиска документов (аналог MongoDB query).
        :param projection: Поля для выборки (аналог MongoDB projection).
        :param limit: Максимальное количество документов для выборки.
        :returns: Список найденных документов.
        """
        coll:pymongo.collection.Collection # type: ignore
        coll = getattr(self, collection)
        cur = coll.find(_normalize(query), projection, limit=limit)
        return list(cur)


    def find_one(self,
                 collection: str,
                 query: Dict[str, Any],
                 projection: Optional[Dict[str, int]] = None
                ) -> Optional[Dict[str, Any]]:
        """
        Ищет один документ в указанной коллекции.
        :param collection: Название коллекции.
        :param query: Фильтр для поиска документа (аналог MongoDB query).
        :param projection: Поля для выборки (аналог MongoDB projection).
        :return: Найденный документ или None, если ничего не найдено.
        """
        coll: pymongo.collection.Collection # type: ignore
        coll = getattr(self, collection)
        obj = coll.find_one(_normalize(query), projection)
        if not obj:
            logger.debug(f"Не найдено в {collection}: {query}")
        return obj


def init_db(cfg:Dict[str, Any]) -> ConfigurableMongoDAO:
    """Возвращает сконфигурированный DAO (используется в инициализации)."""
    uri = cfg["db_host"]
    db_user = cfg["db_user"]
    db_pass = cfg["db_password"]
    db_name = cfg["db_name"]
    db_cfg_path = Path(cfg["db_config_path"] or Path(__file__).parent / "config/db_config.yaml")
    collections_cfg = _load_db_config_yaml(db_cfg_path)

    client = MongoClient(uri, username=db_user, password=db_pass, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except pymongo.errors.ServerSelectionTimeoutError: # type: ignore
        raise ValueError('DB unavailable')
    return ConfigurableMongoDAO(client=client, db_name=db_name, collections_config=collections_cfg)

def _load_db_config_yaml(path: Path) -> Dict[str, Dict[str, Any]]:
    """Читаем config/db_config.yaml и собираем структуру индексов."""
    if not path.exists():
        raise FileNotFoundError(f"DB config not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    cols = data.get("collections", {}) or {}
    out: Dict[str, Dict[str, Any]] = {}
    for coll_name, spec in cols.items():
        idx_list = []
        for idx in spec.get("indexes", []) or []:
            keys_raw = idx.get("keys", [])
            keys_parsed: List[tuple[str, int]] = []
            for k in keys_raw:
                if not (isinstance(k, (list, tuple)) and len(k) == 2):
                    raise ValueError(f"Invalid key spec for {coll_name}: {k}")
                keys_parsed.append((str(k[0]), _dir_from_str(str(k[1]))))
            idx_list.append({**{k: v for k, v in idx.items() if k != "keys"}, "keys": keys_parsed})
        out[coll_name] = {"indexes": idx_list}
    return out