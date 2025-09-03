from __future__ import annotations
import logging
import pymongo
from dataclasses import asdict, dataclass, field, is_dataclass 
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from utils.common import log_error, load_yaml

logger = logging.getLogger(__name__)  # наследует конфиг из watchdog.py


def check_db_connection(host:str,
                        db_user:str,
                        db_pass:str,
                        db_timeout:Optional[str]='1000'
                       ) -> pymongo.MongoClient:
    """Проверка доступности БД
    ------------------
    :returns: объект MongoDB
    """
    # Ждём отклика по таймауту
    mongo_db = pymongo.MongoClient(
                                   host=host,
                                   username = db_user,
                                   password = db_pass,
                                   serverSelectionTimeoutMS=int(db_timeout) # pyright: ignore[reportArgumentType]
                                  )
    try: 
        mongo_db.server_info()
        logger.info("Connected to MongoDB")
    except pymongo.errors.ServerSelectionTimeoutError as e: # pyright: ignore[reportAttributeAccessIssue]
        log_error(error_type=e, error_message="MongoDB timeout!")
    finally:
        return mongo_db


# Dataclass-DAO с динамическими коллекциями и индексами.
# Конфиг индексов вынесен в YAML (db_config.yaml) для переиспользования
# между разными пайплайнами/проектами.
# ======================================================================
@dataclass(slots=True)
class MongoDAO:
    """
    Универсальный DAO:
      • динамически создаёт атрибуты коллекций (self.files, self.batches, ...)
      • создаёт индексы из конфигурации (загружается из YAML)
      • предоставляет универсальные upsert/find
    """
    client: pymongo.MongoClient
    db_name: str
    cfg_path: Path
    collections_config: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    collections: Dict[str, pymongo.collection.Collection] = field(init=False, default_factory=dict) # type: ignore
    db: pymongo.database.Database = field(init=False) # type: ignore

    def __post_init__(self) -> None:
        self.db = self.client[self.db_name]
        # Навешиваем коллекции как атрибуты DAO
        for coll_name in self.collections_config.keys():
            self.collections[coll_name] = self.db.get_collection(coll_name)
        # Из конфига выгружаем данные по индексам коллекций
        
        # Идемпотентно создаём индексы
        self.ensure_indexes()


    def ensure_indexes(self) -> None:
        """
        Создаём индексы для всех коллекций из конфига.
        """
        idx_data: dict
        # Выгружаем индексы из конфига
        indexes = load_db_config_yaml(self.cfg_path)
        for coll_name, collection in self.collections.items():
            idx_data = indexes.get(coll_name, {}).get('indexes', {})
            keys = idx_data.get("keys", [])
            name = idx_data.get("name")
            # Остальные параметры индекса (unique, sparse, partialFilterExpression, ...)
            kwargs = {k: v for k, v in idx_data.items() if k not in {"keys", "name"}}
            if name:
                kwargs["name"] = name
            collection.create_index(keys, **kwargs)


    # -------------------- универсальные операции --------------------
    def upsert(self, collection: str, key: Dict[str, Any], doc: Dict[str, Any]) -> None:
        """Idempotent upsert с автозаполнением created_at/updated_at."""
        coll = getattr(self, collection)
        d = _normalize(doc)
        now = datetime.now(timezone.utc)
        d.setdefault("updated_at", now)
        coll.update_one(key, {"$set": d, "$setOnInsert": {"created_at": now}}, upsert=True)

    def find(self, collection: str, query: Dict[str, Any],
             projection: Optional[Dict[str, int]] = None, limit: int = 0) -> List[Dict[str, Any]]:
        """Поиск с проекцией и лимитом."""
        coll = getattr(self, collection)
        cur = coll.find(_normalize(query), projection, limit=limit)
        return list(cur)

# -------------------------- удобная обёртка ----------------------------
'''def ensureIndexes() -> None:
    """
    Инициализация индексов:
      • коннект к Mongo по .env
      • загрузка db_config.yaml (путь в DB_CONFIG_PATH, по умолчанию config/db_config.yaml)
      • ping
      • создание индексов по YAML
    """
    from utils.common import env_var  # локальный импорт во избежание циклов
    # Подключаемся к Mongo
    uri = env_var("MONGO_URI")
    db_user = env_var("DB_USER")
    db_pass = env_var("DB_PASSWORD")
    db_name = env_var("DB_NAME")
    # Грузим конфиг индексов
    cfg_path = Path(env_var("DB_CONFIG_PATH") or "config/db_config.yaml")
    collections_cfg = _load_db_config_yaml(cfg_path)

    client = pymongo.MongoClient(uri, username=db_user, password=db_pass, serverSelectionTimeoutMS=2000)
    client.admin.command("ping")
    dao = MongoDAO(client=client, db_name=db_name, collections_config=collections_cfg)
    # ensure_indexes() уже вызван в __post_init__, но повторный вызов безопасен
    dao.ensure_indexes()'''


# ----------------------- нормализация данных для загрузки в MongoDB ------------------------
def load_db_config_yaml(path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Читает YAML вида:
    collections:
        <name>:
        indexes:
            - name: ix_name
            unique: true
            keys: [["field1","ASC"], ["field2","DESC"]]
    Возвращает dict: { <name>: {"indexes": [ {"keys":[(f,dir),...], ...}, ... ] } }
    """
    collections: Dict[str, Dict[str, List[Dict[str, Any[bool|list]]]]]
    out: Dict[str, Dict[str, Any]] = {}
    
    collections = load_yaml(path, critical=True, subsection='collections')
    for coll_name, spec in collections.items():
        idx_list = []
        for idx in spec.get("indexes", []):
            keys_raw = idx.get("keys", [])
            keys_parsed: List[Tuple[str, int]] = []
            for k in keys_raw:
                # k: ["field", "ASC"|"DESC"]
                if not (isinstance(k, (list, tuple)) and len(k) == 2):
                    raise ValueError(f"Invalid key spec for {coll_name}: {k}")
                keys_parsed.append((str(k[0]), _dir_from_str(str(k[1]))))
            idx_list.append({**{k: v for k, v in idx.items() if k != "keys"}, "keys": keys_parsed})
        out[coll_name] = {"indexes": idx_list}
    return out


def _to_utc(dt: datetime) -> datetime:
    """Все даты в UTC (BSON Date)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _normalize(value: Any) -> Any:
    """
    Рекурсивно приводим к BSON-дружелюбному виду:
    - dataclass -> dict (через asdict)
    - Path -> str (POSIX)
    - datetime -> UTC datetime
    - set/tuple -> list
    - dict/list -> обрабатываем рекурсивно
    """
    if is_dataclass(value):
        value = asdict(value) # type: ignore
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, datetime):
        return _to_utc(value)
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize(v) for v in value]
    return value

# ----------------------- загрузка db_config из YAML ---------------------
def _dir_from_str(s: str) -> int:
    s = s.strip().upper()
    if s == "ASC":
        return pymongo.ASCENDING
    if s == "DESC":
        return pymongo.DESCENDING
    raise ValueError(f"Unknown index direction: {s}")


# GPT5
# ======================================================================
# Быстрая проверка ФС при инициализации:
#   1) Читаем снапшоты директорий из Mongo (коллекция 'directories')
#   2) Вычисляем список "листовых" директорий на диске (до заданной глубины)
#   3) Сравниваем по mtime директории (и "тихому периоду") и строим план:
#        - unchanged: можно не трогать (индекс из БД актуален)
#        - changed:   нужно пересканировать только эти директории
#        - new:       новые директории (их не было в БД) — сканируем
#        - missing:   были в БД, но исчезли на диске — пометить как удалённые
#
# Замечания:
#  - Сканирование директорий выполняется ОСНОВНО ТОЛЬКО ПО ДИРЕКТОРИЯМ
#    и с ограничением глубины → I/O минимален.
#  - Мы не считаем файлы/размеры на этом шаге, чтобы не грузить диск.
#    Это делает твой детальный индексатор ТОЛЬКО для changed/new.
#  - Для надёжности используем "тихий период" quiet_sec: если mtime
#    слишком свежий — считаем, что директория ещё «пишется», отложим.
# ======================================================================
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Iterable, Tuple, Optional
import os
import time
import logging

logger = logging.getLogger(__name__)

# ----------------------------- Снапшот из БД -----------------------------
@dataclass(frozen=True, slots=True)
class DirSnapshot:
    """Лёгкое представление директории из Mongo 'directories'."""
    path: Path          # абсолютный путь директории на диске
    files_total: int    # агрегат на момент индексации (для информации)
    size_total: int     # агрегат на момент индексации (для информации)
    modified_ns: int    # st_mtime_ns директории на момент индексации
    fingerprint: str    # твой агрегированный отпечаток (если есть), можно пустой

# -------------------------- Загрузка из Mongo ----------------------------
def load_fs_snapshots_from_db(dao) -> Dict[str, DirSnapshot]:
    """
    Читает коллекцию 'directories' и возвращает словарь: {path:str -> DirSnapshot}.
    Ожидаемые поля документа: path_ (строка), files_total, size_total, modified_ns, fingerprint.
    Имя поля 'path_' выбрано под твой index; при желании можно сменить.
    """
    try:
        coll = getattr(dao, "directories", None) or dao.db.get_collection("directories")
        # Берём только нужные поля
        cur = coll.find({}, {
            "_id": 0,
            "path_": 1,
            "files_total": 1,
            "size_total": 1,
            "modified_ns": 1,
            "fingerprint": 1,
        })
        out: Dict[str, DirSnapshot] = {}
        for d in cur:
            p = Path(d["path_"]).resolve()
            out[p.as_posix()] = DirSnapshot(
                path=p,
                files_total=int(d.get("files_total", 0) or 0),
                size_total=int(d.get("size_total", 0) or 0),
                modified_ns=int(d.get("modified_ns", 0) or 0),
                fingerprint=str(d.get("fingerprint", "") or ""),
            )
        logger.info("Loaded %d directory snapshots from DB", len(out))
        return out
    except Exception:
        logger.exception("Failed to load 'directories' snapshots from Mongo")
        return {}

# ------------------------ Обход директорий по глубине --------------------
def iter_dirs_at_depth(root: Path, depth: int) -> Iterable[Path]:
    """
    Итерируется ТОЛЬКО по директориям до фиксированной глубины `depth` (0 => сам root).
    Использует os.scandir для минимизации syscalls на больших деревьях.
    """
    if depth < 0:
        return
    # Нормализуем
    try:
        root = root.resolve()
    except Exception:
        root = Path(os.path.abspath(str(root)))

    # BFS-поиск по уровням (без рекурсии в Python)
    queue: List[Tuple[Path, int]] = [(root, 0)]
    while queue:
        current, lvl = queue.pop(0)
        # Отдаём директорию, если она именно этого уровня
        if lvl == depth:
            yield current
            continue
        # Иначе спускаемся глубже
        try:
            with os.scandir(current) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        queue.append((Path(entry.path), lvl + 1))
        except FileNotFoundError:
            # Могли удалить в процессе
            logger.debug("Directory disappeared during scan: %s", current)
        except PermissionError:
            logger.warning("No permission to read directory: %s", current)
        except OSError:
            logger.exception("OS error while scanning directory: %s", current)

# ----------------------- Быстрый план обновления ФС ----------------------
def plan_fast_fs_check(
    source_root: Path,
    dao,
    *,
    leaf_depth: int = 3,
    quiet_sec: int = 60,
) -> Dict[str, List[Path]]:
    """
    Строит план «что делать с поддеревьями» на старте:
      - unchanged:     директории, которые можно НЕ пересканировать
      - changed:       директории, которые нужно пересканировать (mtime директории изменился)
      - new:           директории, которых не было в БД (пересканировать)
      - missing:       директории, которые были в БД, но пропали на диске (пометить как удалённые)

    Аргументы:
      source_root — корень исходных данных
      leaf_depth  — уровень, на котором лежат «листовые» директории с файлами (для ONT это обычно 3)
      quiet_sec   — «тихий период»: если директория изменена меньше, чем quiet_sec назад, считаем её ещё «горячей»
                    и относим к changed (но можно отложить обработку на следующий тик).

    Возвращает словарь списков Path.
    """
    now_ns = time.time_ns()
    db_index = load_dir_snapshots_from_db(dao)
    db_paths = set(db_index.keys())

    disk_paths: List[Path] = []
    for d in iter_dirs_at_depth(Path(source_root), leaf_depth):
        disk_paths.append(d)

    disk_set = set(p.as_posix() for p in disk_paths)

    # Категории
    unchanged: List[Path] = []
    changed:   List[Path] = []
    new:       List[Path] = []
    missing:   List[Path] = []

    # 1) Разбираем то, что есть на диске
    for p in disk_paths:
        p_key = p.as_posix()
        snap = db_index.get(p_key)
        try:
            st = p.stat()
            mtime_ns = st.st_mtime_ns
        except FileNotFoundError:
            # Могли удалить прямо во время прохода
            continue
        except PermissionError:
            logger.warning("No permission to stat directory: %s", p)
            continue
        except OSError:
            logger.exception("OS error stat'ing directory: %s", p)
            continue

        # Новый каталог — не встречался в БД
        if snap is None:
            new.append(p)
            continue

        # Слишком свежий? Считаем «изменённым», но реальный детальный скан можно отложить.
        age_sec = (now_ns - mtime_ns) / 1e9
        if mtime_ns != snap.modified_ns or age_sec < quiet_sec:
            changed.append(p)
        else:
            unchanged.append(p)

    # 2) Те, что были в БД, но пропали на диске
    for p_key in db_paths - disk_set:
        missing.append(Path(p_key))

    logger.info(
        "FS plan: unchanged=%d, changed=%d, new=%d, missing=%d",
        len(unchanged), len(changed), len(new), len(missing)
    )
    return {
        "unchanged": unchanged,
        "changed": changed,
        "new": new,
        "missing": missing,
    }

# -------------------------- Помощник: upsert в БД ------------------------
def upsert_directory_snapshot(
    dao,
    *,
    path: Path,
    files_total: int,
    size_total: int,
    modified_ns: int,
    fingerprint: str,
) -> None:
    """
    Idempotent upsert снапшота директории в коллекцию 'directories'.
    Поля под твой YAML-индекс:
      - path_         (уникальный ключ)
      - files_total
      - size_total
      - modified_ns
      - fingerprint
      - updated_at / created_at
    """
    try:
        coll = getattr(dao, "directories", None) or dao.db.get_collection("directories")
        key = {"path_": Path(path).resolve().as_posix()}
        doc = {
            "path_": key["path_"],
            "files_total": int(files_total),
            "size_total": int(size_total),
            "modified_ns": int(modified_ns),
            "fingerprint": str(fingerprint),
        }
        now_ms = int(time.time() * 1000)
        coll.update_one(
            key,
            {"$set": {**doc, "updated_at": now_ms}, "$setOnInsert": {"created_at": now_ms}},
            upsert=True,
        )
    except Exception:
        logger.exception("Failed to upsert directory snapshot: %s", path)

# ----------------- Помощник: пометить директорию «удалённой» -------------
def mark_directory_missing(dao, path: Path) -> None:
    """
    Если директория из БД пропала с диска — помечаем флагом is_deleted=true.
    Это безопаснее, чем удалять документ (можно аудировать).
    """
    try:
        coll = getattr(dao, "directories", None) or dao.db.get_collection("directories")
        key = {"path_": Path(path).resolve().as_posix()}
        coll.update_one(key, {"$set": {"is_deleted": True, "deleted_at": int(time.time() * 1000)}})
    except Exception:
        logger.exception("Failed to mark directory missing: %s", path)
```

### Как этим пользоваться в `init_fs()` (идея)

* На старте: если `directories` в БД **не пусты**, вызываем `plan_fast_fs_check(...)`.
* Затем:

  * **ничего** не делаем для `unchanged`;
  * для `changed` и `new` — запускаем твой индексатор **только** по этим листовым директориям;
  * для `missing` — вызываем `mark_directory_missing()` и при желании помечаем связанные файлы/батчи/образцы как удалённые/устаревшие (это отдельная логика).
* После пересканирования конкретной директории — обновляем снапшот `upsert_directory_snapshot()` агрегатами и `modified_ns` директории (и свой `fingerprint`).

Если хочешь, дальше добавлю:

* обёртку Prefect-задач: `@task plan_fast_fs_check(...)` + `@task rescan_dir(...)` и маппинг по спискам;
* «ускоренный скан листовой директории» (только внутри `dir`, без рекурсии) с подсчётом `files_total/size_total/max_mtime_ns` и вызовом твоего `SourceFileMeta`/симлинков.
