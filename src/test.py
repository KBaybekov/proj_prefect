from pathlib import Path
from typing import Optional, Dict, List

def lineage_map(source_d: Path, subdir: Path) -> Dict[Path, dict]:
    """
    Строит словарь для цепочки директорий от source_d до subdir (включая обе):
      key: Path к узлу
      value: {"parent": Optional[Path], "kids": List[Path]}  # kids = следующий узел цепочки (0/1)
    """
    root = source_d.resolve()
    target = subdir.resolve()

    # Проверка вложенности
    try:
        rel = target.relative_to(root)
    except ValueError:
        raise ValueError(f"{target} не лежит внутри {root}")

    # Пошагово собираем цепочку
    parts = list(rel.parts)  # например: ["aa", "bb", "cc"]
    chain: List[Path] = [root]
    cur = root
    for part in parts:
        cur = cur / part
        chain.append(cur)

    # Формируем словарь parent/kids
    out: Dict[Path, dict] = {}
    for i, p in enumerate(chain):
        parent: Optional[Path] = chain[i-1] if i > 0 else Path()
        kids: List[Path] = [chain[i+1]] if i < len(chain) - 1 else []
        out[p] = {"parent": parent, "kids": kids}
    return out


dd = {Path('/source'): {Path('/source/aa'): {Path('/source/aa/bb'): {}, Path('/source/aa/cc'): {Path('/source/aa/cc/ee'): {Path('/source/aa/cc/ee/ff'):{}}}}}}
"""aa = []
def loop_dir_dict(dds:dict, aad:set) -> list:
    for parent, kids in dds.items():
        aad.add(parent)
        if kids:
            for kid in kids.keys():
                aad = loop_dir_dict(dds[parent], aad)
        else:
            return aad
    return aad
aa = loop_dir_dict(dd, aa)"""
#print(lineage_map(Path('/source'), Path('/source/aa/cc/ee/ff')))
print(Path('.').stat().st_size)

        