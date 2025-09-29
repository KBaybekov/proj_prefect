# -*- coding: utf-8 -*-
"""
Заглушка обработки: создаёт <sample>.done в RESULT_DIR/<sample>/
"""
from __future__ import annotations
from pathlib import Path
import sys, time
from utils.logger import get_logger

logger = get_logger(name=__name__)


def main(sample: str, input_dir: str, output_dir: str, tmp_dir: str) -> int:
    out = Path(output_dir) / sample
    out.mkdir(parents=True, exist_ok=True)
    # имитация работы
    time.sleep(2)
    (out / f"{sample}.done").write_text("OK\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("usage: process.py <sample> <in> <out> <tmp>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]))
