# -*- coding: utf-8 -*-
"""
MongoDB helpers
"""

from __future__ import annotations

import pymongo
from pymongo import MongoClient

from typing import Any, Dict, List, Mapping, Optional
from datetime import datetime, timezone
from threading import Timer

from enum import Enum
from dataclasses import dataclass, field, fields, asdict, is_dataclass
from pathlib import Path
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
    _cfg: Dict[str, Any] 
    _client: MongoClient = field(default_factory=MongoClient)
    db: pymongo.database.Database = field(init=False) # type: ignore
    poll_interval: int = 10
    db_timer:Optional[Timer] = field(default=None)

    def init_dao(
                 self
                ) -> None:
        self._get_mongo_client()
        self.db = self._client[self._cfg['db_name']]
        self._check_collections()
        return None
    
    def start_dao(
                  self
                 ) -> None:
        """
        Переводит DAO в режим мониторинга.
        """
        logger.info("Запуск мониторинга базы данных...")
        self._monitor_db()

    def _monitor_db(
                    self
                   ) -> None:
        """
        Периодически проверяет изменения в выполнении заданий.
        Выгружает изменения в БД.
        """
        def __check_db():
            ping_mongo(self._client)
            self._monitor_db()
        self.db_timer = Timer(self.poll_interval, __check_db)
        self.db_timer.start()

    def _get_mongo_client(
                          self                              
                         ) -> None:   
        self._client = MongoClient(
                                   host=self._cfg['host'],
                                   username=self._cfg['user'],
                                   password=self._cfg['password'],
                                   serverSelectionTimeoutMS=int(self._cfg['timeout'])
                                  )
        ping_mongo(self._client)
        return None

    def _check_collections(
                           self
                          ) -> None:
        for coll_name in self._cfg['collections'].keys():
            if not hasattr(self, coll_name):
                setattr(self, coll_name, self.db.get_collection(coll_name))
            self._ensure_indexes(coll_name)
        return None

    def _ensure_indexes(
                        self,
                        coll_name:str
                       ) -> None:
        """Идёмпотентно создаём индексы по конфигу YAML."""
        coll: Optional[pymongo.collection.Collection] # type: ignore
        coll = getattr(self, coll_name, None)
        if coll != None:
            coll_cfg = self._cfg['collections'].get(coll_name, {})
            for spec in coll_cfg.get("indexes", []):
                keys = spec.get("keys", [])
                name = spec.get("name")
                kwargs = {k: v for k, v in spec.items() if k not in {"keys", "name"}}
                if name:
                    kwargs["name"] = name
                coll.create_index(keys, **kwargs)

    def insert_many(
                    self,
                    collection: str,
                    documents: List[Dict[str, Any]]
                   ) -> None:
        """
        Вставляет множество новых документов в указанную коллекцию.
        
        :param collection: Название коллекции.
        :param documents: Список документов для вставки.
        """
        coll: pymongo.collection.Collection # type: ignore
        coll = getattr(self, collection)
        if not documents:
            logger.debug(f"Нет документов для вставки в коллекцию {collection}")
            return None
        
        # Нормализуем и вставляем
        normalized_docs = [_normalize(doc) for doc in documents]
        result = coll.insert_many(normalized_docs)
        logger.info(f"Добавлено {len(result.inserted_ids)} новых документов в коллекцию {collection}")

    def update_many(
                    self,
                    collection: str,
                    query: Dict[str, Any],
                    doc: Dict[str, Any]
                   ) -> None:
        """
        Обновляет/вставляет множество документов в указанной коллекции.
        
        :param collection: Название коллекции.
        :param updates: Словарь вида {query: document}, где:
            - query: фильтр для поиска документа (аналог MongoDB query).
            - document: данные для обновления/вставки.
        """
        coll: pymongo.collection.Collection # type: ignore
        coll = getattr(self, collection)
        normalized_doc:Dict[str, Any] = _normalize(doc)
        now = datetime.now(timezone.utc)
        # Добавляем временные метки
        normalized_doc.setdefault("updated_at_DB", now)
            
        # Используем $setOnInsert для установки created_at_DB при вставке
        result = coll.update_many(
                                  filter=query,
                                  update={
                                          "$set": normalized_doc,
                                          "$setOnInsert": {"created_at_DB": now}
                                         }
                                 )
        logger.debug(f"Подходящих записей: {result.matched_count}. Обновлено {result.modified_count} записей в коллекцию {collection}")

    def update_one(
                   self,
                   collection: str,
                   query: Dict[str, Any],
                   doc: Dict[str, Any]
                  ) -> None:
        """
        Обновляет один документ в указанной коллекции.

        :param collection: Название коллекции.
        :param query: Фильтр для поиска документа (аналог MongoDB query).
        :param doc: Данные для обновления.
        """ 
        coll: pymongo.collection.Collection # type: ignore
        coll = getattr(self, collection)
        normalized_doc:Dict[str, Any] = _normalize(doc)
        now = datetime.now(timezone.utc)
        # Добавляем временные метки
        normalized_doc.setdefault("updated_at_DB", now)
        result = coll.update_one(
                                 filter=query,
                                 update={"$set": normalized_doc}
                                 )
        if result.modified_count != 1:
            logger.error(f"При попытке обновления одного документа обновлено: {result.modified_count}.\nЗапрос: {query}.\nДанные: {doc}")
        else:
            logger.debug(f"Успешно обновлен 1 документ при запросе {query}.\nДанные: {doc}")

    def upsert_one(
                   self,
                   collection: str,
                   key: Dict[str, Any],
                   doc: Dict[str, Any]
                  ) -> None:
        """
        Обновляет или создаёт новый документ в указанной коллекции.
        """
        coll: pymongo.collection.Collection # type: ignore

        coll = getattr(self, collection)
        d: Dict[str, Any] = _normalize(doc)
        now = datetime.now(timezone.utc)
        d.setdefault("updated_at_DB", now)
        coll.update_one(key, {"$set": d, "$setOnInsert": {"created_at_DB": now}}, upsert=True)

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
                ) -> Dict[str, Any]:
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
            return {}
        return obj
    
    def delete_one(
               self,
               collection: str,
               query: Dict[str, Any]
              ) -> None:
        """
        Удаляет один документ из указанной коллекции по фильтру.

        :param collection: Название коллекции.
        :param query: Фильтр для поиска документа (аналог MongoDB query).
        """
        coll: pymongo.collection.Collection = getattr(self, collection) # type: ignore
        result = coll.delete_one(_normalize(query))
        
        if result.deleted_count == 1:
            logger.debug(f"Успешно удалён 1 документ из коллекции {collection} при запросе {query}")
        elif result.deleted_count == 0:
            logger.debug(f"Нет документов для удаления в коллекции {collection} при запросе {query}")
        else:
            logger.warning(f"Неожиданное количество удалённых документов ({result.deleted_count}) в коллекции {collection}")

    def stop_dao(self) -> None:
        """
        Корректно останавливает DAO:
        - останавливает таймер мониторинга БД
        - закрывает соединение с MongoDB
        - освобождает ресурсы
        """
        logger.info("Остановка ConfigurableMongoDAO...")

        # 1. Останавливаем таймер мониторинга
        if self.db_timer is not None:
            logger.debug("Остановка таймера мониторинга БД...")
            self.db_timer.cancel()
            self.db_timer = None
            logger.debug("Таймер мониторинга остановлен")

        # 2. Закрываем MongoClient
        if self._client is not None:
            logger.debug("Закрытие соединения с MongoDB...")
            self._client.close()
            self._client = MongoClient()  # заменяем на пустой, чтобы избежать повторного close()
            logger.debug("Соединение с MongoDB закрыто")

        logger.info("ConfigurableMongoDAO остановлен корректно")

def ping_mongo(
               client: pymongo.MongoClient
              ) -> None:
    """
    Проверяет доступность MongoDB.
    """
    try:
        client.admin.command("ping")
    except pymongo.errors.ServerSelectionTimeoutError: # type: ignore
        raise ValueError('DB unavailable')