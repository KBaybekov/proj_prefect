# -*- coding: utf-8 -*-
"""
Модуль для хранения и управления метаданными результатов выполнения пайплайнов.

Класс ResultMeta предназначен для сбора, сериализации и хранения результатов
анализа образцов, полученных в ходе выполнения различных биоинформатических пайплайнов.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from modules.logger import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class ResultMeta:
    """
    Класс для хранения результатов выполнения пайплайнов по образцу.

    Используется для агрегирования выходных данных, метрик и статусов
    из различных пайплайнов (например, QC, сборка, аннотация) в единую структуру.
    Хранится в коллекции 'results' базы данных, ключ — sample_id.

    Поддерживает версионирование через fingerprint и отслеживание времени обновления.
    """
    name: str
    """
    Человекочитаемое имя образца (например, 'Sample_001').
    """

    fingerprint: str
    """
    Криптографический отпечаток (хэш) образца, вычисляемый на основе его данных.
    Используется для проверки целостности и идентификации версий.
    """

    sample_id: str
    """
    Уникальный идентификатор образца в системе.
    Используется как ключ для поиска и агрегации результатов.
    """

    results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    """
    Словарь результатов выполнения пайплайнов.
    
    Формат: 
    {
        "pipeline_name": {
            "status": "completed",          # Статус выполнения
            "metrics": {"depth": 50, ...},  # Ключевые метрики
            "outputs": [...],               # Пути к выходным файлам
            "version": "1.0",               # Версия пайплайна
            # ... любые другие данные пайплайна
        },
        ...
    }
    """

    created: Optional[datetime] = field(default=None)
    """
    Дата и время создания записи (при создании первого задания с образцом).
    """

    modified: Optional[datetime] = field(default=None)
    """
    Дата и время последнего обновления записи.
    Обновляется при добавлении результатов нового пайплайна или перезапуске существующего.
    """

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any]
                 ) -> 'ResultMeta':
        """
        Создаёт экземпляр ResultMeta из документа MongoDB.

        :param doc: Словарь с данными из коллекции 'results'.
        :type doc: Dict[str, Any]
        :return: Инициализированный объект ResultMeta.
        :rtype: ResultMeta
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
        Преобразует объект ResultMeta в словарь для сохранения в MongoDB.

        :return: Сериализованный словарь со всеми полями объекта.
        :rtype: Dict[str, Any]
        """
        dict_obj = self.__dict__
        return dict_obj
