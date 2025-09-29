# -*- coding: utf-8 -*-
"""
Работа со Slurm через subprocess
"""
from __future__ import annotations

import subprocess
import os
from dataclasses import dataclass
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(name=__name__)

def run_shell_cmd(cmd: str, timeout: int | None = None) -> dict:
    """Выполнить shell-команду, вернуть rc/stdout/stderr."""
    try:
        p = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, executable="/bin/bash"
        )
        out, err = p.communicate(timeout=timeout)
        return {"rc": p.returncode, "stdout": out.strip(), "stderr": err.strip()}
    except subprocess.TimeoutExpired:
        p.kill() # type: ignore
        out, err = p.communicate() # type: ignore
        return {"rc": -9, "stdout": out.strip(), "stderr": "TIMEOUT"}


def check_slurm() -> TaskScheduler:
    """Проверка доступности Slurm (без accounting)."""
    res = run_shell_cmd("sinfo -h -o '%T'", timeout=2)
    if res["rc"] == 0 and (("up" in res["stdout"].lower()) or res["stdout"].strip() != ""):
        logger.info("Slurm OK")
        return "slurm"
    logger.error("Slurm not available: %s", res)
    raise RuntimeError("Slurm not available")
    !!!


def submit_sample_job(sample_name: str, input_dir: str, output_dir: str, tmp_dir: str) -> str:
    job_dir = Path(output_dir) / sample_name
    job_dir.mkdir(parents=True, exist_ok=True)
    script = job_dir / "job.sh"

    script.write_text(f"""#!/bin/bash
#SBATCH --job-name=proc_{sample_name}
#SBATCH --output={job_dir}/slurm-%j.out

set -euo pipefail
python -m utils.process '{sample_name}' '{input_dir}' '{output_dir}' '{tmp_dir}'
""", encoding="utf-8")
    os.chmod(script, 0o755)

    # Минимальный набор: только --parsable + путь к скрипту
    res = run_shell_cmd(f"sbatch --parsable {script}")
    if res["rc"] != 0 or not res["stdout"]:
        logger.error("sbatch failed: %s", res)
        raise RuntimeError("sbatch failed")
    job_id = res["stdout"].strip()
    logger.info("Submitted %s => job_id=%s", sample_name, job_id)
    return job_id


@dataclass
class TaskScheduler:
    """
    Класс для работы с планировщиком заданий Slurm
    """