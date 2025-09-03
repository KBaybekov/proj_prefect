import hashlib, json
from pathlib import Path
from utils.filesystem import load_yaml  # ваш loader

class RefStore:
    def __init__(self, paths: dict[str, Path]):
        self.paths = paths
        self.cache: dict[str, dict] = {}
        self.mtime: dict[str, float] = {}
        self.version = ""

    def _calc_version(self) -> str:
        h = hashlib.blake2s()
        for k in sorted(self.cache):
            h.update(k.encode()); h.update(b'|')
            h.update(json.dumps(self.cache[k], sort_keys=True).encode()); h.update(b'\n')
        return h.hexdigest()

    def get(self) -> tuple[dict, dict, dict, str]:
        reloaded = False
        for key, p in self.paths.items():
            mt = p.stat().st_mtime
            if self.mtime.get(key) != mt:
                self.cache[key] = load_yaml(str(p), critical=True)
                self.mtime[key] = mt
                reloaded = True
        if reloaded:
            self.version = self._calc_version()
        return (
            self.cache.get('pore_data', {}),
            self.cache.get('models', {}),
            self.cache.get('available_modifications', {}),
            self.version,
        )

# инициализация (один раз)
REFS = RefStore({
    'pore_data': Path('data/pores_n_chemistry.yaml'),
    'models': Path('data/dorado_models.yaml'),
    'available_modifications': Path('data/available_modifications.yaml'),
})