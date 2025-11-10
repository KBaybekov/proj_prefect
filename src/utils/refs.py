import hashlib, json
from pathlib import Path
from utils.common import load_yaml
from typing import List
from utils.logger import get_logger

logger = get_logger(name=__name__)

# !!! Доработать 
class RefStore:
    """
    Хранилище ссылок на файлы конфигурации.
    Отслеживает изменение конфигурационных файлов.
    """
    def __init__(self, paths: dict[str, Path]):
        self.paths: dict[str, Path] = paths
        self.cache: dict[str, dict] = {}
        self.mtime: dict[str, float] = {}
        self.versions: dict[str, str] = {}

    def _calc_version(self, key:str) -> str:
        h = hashlib.blake2s()
        h.update(key.encode()); h.update(b'|')
        h.update(json.dumps(self.cache[key], sort_keys=True).encode()); h.update(b'|')
        return h.hexdigest()

    def get(self) -> List[dict]:
        for key, p in self.paths.items():
            reloaded = False
            mt = p.stat().st_mtime
            if self.mtime.get(key) != mt:
                self.cache[key] = load_yaml(p, critical=True)
                self.mtime[key] = mt
                reloaded = True
            if reloaded:
                self.versions[key] = self._calc_version(key)
        data = [self.versions]
        data.extend([v for v in self.cache.values()])
        return data
                
# инициализация (один раз)
ref_dicts = {k:Path(v) for k,v in
             load_yaml(Path('config/reference_files.yaml'),
                       critical=True).items()}
REFS = RefStore(ref_dicts)

    