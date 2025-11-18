# -*- coding: utf-8 -*-

from __future__ import annotations
from utils.logger import get_logger
from os import scandir
from pathlib import Path
from typing import List, Tuple

logger = get_logger(name=__name__)

def remove_symlink(
                   symlink: Path
                  ) -> None:
    """
    Если симлинк существует, то удаляем его

    :param symlink: Путь к симлинку
    """
    if symlink.exists():
        if symlink.is_symlink():
            symlink.unlink()
            logger.debug(f"Симлинк {symlink} удалён.")
        else:
            logger.error(f"{symlink} - не симлинк")
    else:
        logger.error(f"Симлинк {symlink} не существует.")
    return None

def get_files_by_extension_in_dir_tree(
                                       dirs: List[Path],
                                       extensions: Tuple[str, ...],
                                       empty_ok: bool = False,
                                       exclude_dirs: Tuple[Path, ...] = (Path('.git'), Path('__pycache__')),
                                      ) -> List[Path]:
    """
    Эффективно собирает файлы по списку корней (без рекурсивной Python-функции, с os.scandir).

    :param dirs: список корневых директорий
    :param extensions: кортеж расширений (без нормализации)
    :param empty_ok: если False и файлов нет — бросаем FileNotFoundError
    :param exclude_dirs: имена каталогов, которые пропускаем
    """
    results: List[Path] = []
    ex_names = {p.name for p in exclude_dirs}

    for root in dirs:
        try:
            root = root.resolve()
        except Exception:
            root = Path(str(root)).absolute()

        stack: List[Path] = [root]
        while stack:
            current = stack.pop()
            try:
                with scandir(current) as it:
                    for e in it:
                        ep = Path(e.path)
                        if e.is_dir(follow_symlinks=False):
                            if ep.name in ex_names:
                                continue
                            stack.append(ep)
                        else:
                            name = ep.name
                            if any(name.endswith(ext) for ext in extensions):
                                results.append(ep)
            except FileNotFoundError:
                continue
            except PermissionError:
                logger.warning("No permission: %s", current)
            except OSError:
                logger.exception("OS error scanning: %s", current)

    if not results and not empty_ok:
        raise FileNotFoundError("Файлы не найдены")
    return results

