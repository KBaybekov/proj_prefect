from typing import Any, Dict, List, Optional
import os
import json
import subprocess
import getpass
from pathlib import Path
from csv import reader as csv_reader
import yaml


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



def get_user_jobs(user: Optional[str] = None) -> Dict[str, dict]:
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
    except Exception:
        return {}

    jobs:List[Dict[str, Any]] = j.get("jobs") or j.get("job") or []
    result = {}
    for job in jobs:
        # tolerant field extraction (squeue json keys can vary by version)
        jid = job.get('job_id') 
        if not jid:
            continue
        entry = {
            "name": job.get('name'),
            "array_job_id": job.get('array_job_id'),
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


def parse_trace_report(trace_report:Path) -> Dict[str, Dict[str, Any]]:
    def extract_columns_tsv(file_path:Path):
        """
        Extract specific columns from TSV file with minimal I/O.

        """
        cols_with_data = {
                          'job_id':2,
                          'name':5,
                          'status':6,
                          'exit_code':7
                         }
        data = {}
        with open(file_path, 'r', newline='') as f:
            reader = csv_reader(f, delimiter='\t')
            next(reader)  # Skip header
            for row in reader:
                if len(row) > max(cols_with_data.values()):  # Ensure row has enough columns
                    data.update({row[cols_with_data['job_id']]:{
                                                            col_name: row[col] for col_name, col
                                                            in cols_with_data.items()
                                                            if col_name != 'job_id'
                                                            }})
                else:
                    print(f"Warning: Row {row} has fewer columns than expected.")
                    continue
        return data
    

    if any([
            not trace_report.exists(),
            not trace_report.is_file(),
            ]):
        print(f"Bad trace report: {trace_report}")
        return {}
    else:
        if trace_report.is_file():
            return extract_columns_tsv(trace_report)
        else:
            print(f"Trace report is not a file: {trace_report}")
            return {}



def get_child_jobs(trace_report:Path) -> List[int]:
    child_jobs = []
    with open(trace_report) as f:
        for line in f:
            splitted = line.split('\t')
            job_id = splitted[2]
            job_name = splitted[5]



with open('tmp/slurm_tasks1.yaml', 'w') as file:
        yaml.dump(get_user_jobs(user='kbajbekov'), file)

# Нам нужно получить минимальную информацию обо всех задачах в пайплайне
# Т.к. Slurm быстро всё удаляет, будем парсить данные из трейса пайплайна и соотносить с полученной информацией
squeue_data = get_user_jobs(user='kbajbekov')
trace_data = parse_trace_report(Path('/home/kbajbekov/raid/kbajbekov/common_share/github/proj_prefect/tmp/trace_report.txt'))