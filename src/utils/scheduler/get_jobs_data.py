from typing import Any, Dict, List, Optional, Union
import os
import json
import subprocess
import getpass
from pathlib import Path
from csv import reader as csv_reader
import yaml
import time
from shlex import split as shlex_split
import datetime


# helpers
def _run_cmd(cmd: List[str], timeout: Optional[int] = 10):
    """Run command, return (returncode, stdout, stderr)."""
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"

def _start_background_cmd(cmd: Union[List[str], str]) -> Optional[tuple[subprocess.Popen[str], int, int]]:
    """
    Запускает команду в фоне и возвращает process, process.pid, job_id.
    """
    if isinstance(cmd, str):
            try:
                cmd = shlex_split(
                                  cmd,
                                  comments=False,
                                  posix=True
                                 )
            except ValueError as e:
                print(f"Ошибка разбора команды '{cmd}': {e}")
                raise
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        job_id = monitor_sbatch_output(process)
        print(f"[PID {process.pid}] Запущена команда: {' '.join(cmd)}")
        return (process, process.pid, job_id)
    except Exception as e:
        print(f"Ошибка при запуске команды {cmd}: {e}")
        return None

def monitor_sbatch_output(proc: subprocess.Popen) -> int:
    """
    Читает stdout процесса построчно в реальном времени.
    """
    job_id = 0
    if proc.stdout:
        try:
            for line in proc.stdout:  # proc.stdout — итератор по строкам
                line = line.strip()
                if not line:
                    continue
                extracted_job_id = extract_slurm_job_id(line)
                if extracted_job_id:
                    job_id = extracted_job_id
                    break
        except Exception as e:
            print(f"Ошибка при чтении stdout: {e}")
        finally:
            proc.stdout.close()
            return job_id  # ID найден — можно прекратить мониторинг

def extract_slurm_job_id(line: str) -> Optional[int]:
    """
    Извлекает ID задачи из строки вида:
        'Submitted batch job 35904'
    """
    if line.startswith("Submitted batch job"):
        parts = line.split()
        if len(parts) == 4 and parts[3].isdigit():
            return int(parts[3])
    return None

def collect_completed_process_exitcode(p:subprocess.Popen) -> Optional[int]:
    """
    Проверяет все фоновые процессы, извлекает результаты завершённых.
    Возвращает список результатов: {pid, returncode, stdout, stderr, cmd}.
    Удаляет завершённые процессы из списка.
    """

    if p.poll() is None:
        return None
    else:
        return p.returncode


def get_user_jobs(user: Optional[str] = None) -> Dict[int, Dict[str, Union[str, int]]]:
    """
    Возвращает словарь job_id -> summary_dict, полученный из `squeue --json -u user`.
    Не вызывает scontrol (чтобы быть лёгким).
    """
    def timestamp_to_datetime(timestamp: int) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(timestamp)

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
            "array_job_id": job.get('array_job_id', {}).get('number'),
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
            "start": timestamp_to_datetime(job.get('start_time', {}).get('number', 0)),
            "limit": timestamp_to_datetime(job.get('end_time', {}).get('number', 0)),
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
# Запускаем процесс
print('starting...')
started_proc = _start_background_cmd(cmd=[
                                       'bash',
                                       '/raid/kbajbekov/common_share/github/proj_prefect/data/submit_slurm_task.sh'
                                       ])
print('started')

if started_proc:
    proc, proc_pid, main_job_id = started_proc
    print(f"Процесс запущен с PID {proc_pid}, job_id {main_job_id}")

while True:
    # Читаем данные из squeue и трейса
    squeue_data = get_user_jobs(user=usr)
    with open('/mnt/cephfs8_rw/nanopore2/test_space/results/7777/45gd/logs/slurm/slurm_squeue.yaml', 'w') as file:
        yaml.dump(squeue_data, file)
    #print(squeue_data)
    print(f"received squeue_data, keys:\n{'\n'.join([str(s) for s in squeue_data.keys()])}")
    # Извлекаем данные о главной задаче
    task_data = squeue_data.get(main_job_id, {})
    # Добавляем данные о дочерних задачах
    child_tasks_data = add_child_jobs(squeue_data, main_job_id)
    task_data['child_jobs'] = child_tasks_data # type: ignore
    #print(task_data)
    with open('/mnt/cephfs8_rw/nanopore2/test_space/results/7777/45gd/logs/slurm/slurm_tasks1.yaml', 'w') as file:
        yaml.dump(task_data, file)

    


    time.sleep(10)


    #['account', 'accrue_time', 'admin_comment', 'allocating_node', 'array_job_id', 'array_task_id', 'array_max_tasks', 'array_task_string', 'association_id', 'batch_features', 'batch_flag', 'batch_host', 'flags', 'burst_buffer', 'burst_buffer_state', 'cluster', 'cluster_features', 'command', 'comment', 'container', 'container_id', 'contiguous', 'core_spec', 'thread_spec', 'cores_per_socket', 'billable_tres', 'cpus_per_task', 'cpu_frequency_minimum', 'cpu_frequency_maximum', 'cpu_frequency_governor', 'cpus_per_tres', 'cron', 'deadline', 'delay_boot', 'dependency', 'derived_exit_code', 'eligible_time', 'end_time', 'excluded_nodes', 'exit_code', 'extra', 'failed_node', 'features', 'federation_origin', 'federation_siblings_active', 'federation_siblings_viable', 'gres_detail', 'group_id', 'group_name', 'het_job_id', 'het_job_id_set', 'het_job_offset', 'job_id', 'job_resources', 'job_size_str', 'job_state', 'last_sched_evaluation', 'licenses', 'licenses_allocated', 'mail_type', 'mail_user', 'max_cpus', 'max_nodes', 'mcs_label', 'memory_per_tres', 'name', 'network', 'nodes', 'nice', 'tasks_per_core', 'tasks_per_tres', 'tasks_per_node', 'tasks_per_socket', 'tasks_per_board', 'cpus', 'node_count', 'tasks', 'partition', 'prefer', 'memory_per_cpu', 'memory_per_node', 'minimum_cpus_per_node', 'minimum_tmp_disk_per_node', 'power', 'preempt_time', 'preemptable_time', 'pre_sus_time', 'hold', 'priority', 'priority_by_partition', 'profile', 'qos', 'reboot', 'required_nodes', 'required_switches', 'requeue', 'resize_time', 'restart_cnt', 'resv_name', 'scheduled_nodes', 'segment_size', 'selinux_context', 'shared', 'sockets_per_board', 'sockets_per_node', 'start_time', 'state_description', 'state_reason', 'standard_input', 'standard_output', 'standard_error', 'stdin_expanded', 'stdout_expanded', 'stderr_expanded', 'submit_time', 'suspend_time', 'system_comment', 'time_limit', 'time_minimum', 'threads_per_core', 'tres_bind', 'tres_freq', 'tres_per_job', 'tres_per_node', 'tres_per_socket', 'tres_per_task', 'tres_req_str', 'tres_alloc_str', 'user_id', 'user_name', 'maximum_switch_wait_time', 'wckey', 'current_working_directory']