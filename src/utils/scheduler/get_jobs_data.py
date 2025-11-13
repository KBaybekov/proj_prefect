from typing import Any, Dict, List, Optional, Union
import os
import json
import subprocess
import getpass
from pathlib import Path
from csv import reader as csv_reader
import yaml
import time


# helpers
def _run_cmd(cmd: List[str], timeout: Optional[int] = 10):
    """Run command, return (returncode, stdout, stderr)."""
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"

def _first_present(d: dict, keys):
    for k in keys:
        if k in d:
            return d[k]
    return None



def get_user_jobs(user: Optional[str] = None) -> Dict[int, Dict[str, Union[str, int]]]:
    """
    Возвращает словарь job_id -> summary_dict, полученный из `squeue --json -u user`.
    Не вызывает scontrol (чтобы быть лёгким).
    """
    if user is None:
        user = os.getenv("USER") or getpass.getuser()
    rc, out, _err = _run_cmd(["squeue", "--json", "-u", user], timeout=15)
    if rc != 0:
        print(f"Ошибка при запросе данных о задачах Slurm: exit_code {rc}")
        return {}

    try:
        j = json.loads(out)
    except Exception as e:
        print("Ошибка при преобразовании stdout squeue в json:", e)
        return {}

    jobs:List[Dict[Union[int, str], Any]] = j.get("jobs") or []
    result = {}
    for job in jobs:
        # tolerant field extraction (squeue json keys can vary by version)
        jid = job.get('job_id') 
        if not jid:
            continue
        entry = {
            "job_id": jid,
            "name": job.get('name'),
            "array_job_id": job.get('array_job_id'),
            "parent_job_id": 0
                                if not job.get('comment', '')
                                else int(job.get('comment', '').removeprefix("parent_job_id ")),
            "nodes": job.get('nodes'),
            "partition": job.get('partition'),
            "priority": job.get('priority', {}).get('number'),
            "status": job.get('job_state', [])[-1],
            "stderr": '' 
                        if job.get('standard_error', '') == '/dev/null'
                        else job.get('standard_error', ''),
            "stdout": '' 
                        if job.get('standard_output', '') == '/dev/null'
                        else job.get('standard_output', ''),
            "exit_code": job.get('exit_code', {}).get('number'),
            "start": job.get('start_time', {}).get('number'),
            "limit": job.get('end_time', {}).get('number'),
            "work_dir": job.get('current_working_directory')
        }
        result[jid] = entry
    return result


def add_child_jobs(squeue_data:Dict[int, Dict[str, Union[str, int]]],
                   main_job_id:int) -> Dict[str, Union[str, int]]:
    child_jobs = {}
    for job_id, job_data in squeue_data.items():
        if job_data['parent_job_id'] == main_job_id:
            child_jobs.update({job_id:job_data})
    return child_jobs


# Нам нужно получить минимальную информацию обо всех задачах в пайплайне
# Т.к. Slurm быстро всё удаляет, будем парсить данные из трейса пайплайна и соотносить с полученной информацией
usr = 'kbajbekov'
trace_f = Path('/home/kbajbekov/raid/kbajbekov/common_share/github/proj_prefect/tmp/trace_report.txt')
main_job_id:int = 10000000


while True:
    # Читаем данные из squeue и трейса
    squeue_data = get_user_jobs(user=usr)
    # Извлекаем данные о главной задаче
    task_data = squeue_data.get(main_job_id, {})
    # Добавляем данные о дочерних задачах
    child_tasks_data = add_child_jobs(squeue_data, main_job_id)
    task_data['child_jobs'] = child_tasks_data # type: ignore
    with open('tmp/slurm_tasks1.yaml', 'w') as file:
        yaml.dump(task_data, file)

    


    time.sleep(10)