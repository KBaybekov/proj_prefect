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
        print("p.poll() пустой")
        return None
    else:
        print(f"p.poll() прочитан, получен exit_code {p.returncode}")
        return p.returncode

def define_task_status_by_exit_code(exit_code: int) -> str:
    if exit_code == 0:
        return "COMPLETED"
    elif exit_code == 124:
        return "TIMEOUT"
    else:
        return "FAILED"

def get_user_jobs(user: Optional[str] = None) -> Dict[int, Dict[str, Any]]:
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
            "work_dir": job.get('current_working_directory')
        }
        for property, squeue_property in {'start':'start_time', 'limit':'end_time'}.items():
            time_val = None
            if entry['status'] == 'RUNNING':
                time_val = timestamp_to_datetime(job.get(squeue_property, {}).get('number'))
            entry.update({property: time_val})
        result[jid] = entry
    return result

def timestamp_to_datetime(timestamp: Optional[int]) -> Optional[datetime.datetime]:
    if timestamp:
        return datetime.datetime.fromtimestamp(timestamp)
    return None

def update_task_data(task_data, squeue_data, is_main_proc:bool):
    
    job_id = task_data['job_id']
    job_data = squeue_data.get(job_id, {})
    if job_data:
        status = job_data.get('status', 'UNKNOWN')
        task_data['status'] = status
        if status == 'RUNNING':
            for property, slurm_property in {'start':'start_time', 'limit':'end_time'}.items():
                if all([not task_data.get(property),
                        slurm_property in job_data]):
                    task_data[property] = job_data[slurm_property]
    else:
        print(f"Процесс {job_id} больше не в squeue, извлекаем exit_code")
        # Для главного процесса
        if is_main_proc:
            print(f"Процесс {job_id} - головной, читаем p.poll()")
            task_data['exit_code'] = collect_completed_process_exitcode(task_data['proc'])
        else:
            # Для дочерних процессов
            exit_code_f = Path(task_data.get('work_dir'), '.exitcode').resolve()
            print(f"Процесс f{job_id} - дочерний. Проверяем {exit_code_f}")
            if exit_code_f.exists():
                print(f"Найден .exitcode в {exit_code_f}:")
            # Читаем первую строку и преобразуем в число
                with open(exit_code_f, 'r') as f:
                    task_data['exit_code'] = int(f.readline().strip())
                print(task_data['exit_code'])
            else:
                print(f"Не найден .exitcode в:\n{job_data['work_dir']}")
    if isinstance(task_data['exit_code'], int):
        task_data['status'] = define_task_status_by_exit_code(task_data['exit_code'])
    return task_data


def get_child_jobs(squeue_data:Dict[int, Dict[str, Union[str, int]]],
                   main_job_id:int,
                   child_jobs:Dict[int, Dict[str, Union[str, int]]]) -> Dict[int, Dict[str, Union[str, int]]]:
    # Получаем дочерние задачи; обновляем данные по уже найденным
    for job_id, job_data in squeue_data.items():
        if job_data['parent_job_id'] == main_job_id:
            if job_id not in child_jobs:
                job_data['created'] = datetime.datetime.now()
                child_jobs.update({job_id:job_data})
            else:
                child_jobs[job_id] = job_data
    # Обновляем данные по уже найденным
    for job_id, job_data in child_jobs.items():
        update_task_data(job_data,squeue_data, False)
    return child_jobs

def write_yaml(data:Dict, path:Path):
    with open(path, 'w') as file:
        yaml.dump(data, file)


# Нам нужно получить минимальную информацию обо всех задачах в пайплайне
# Т.к. Slurm быстро всё удаляет, будем парсить данные из трейса пайплайна и соотносить с полученной информацией
usr = 'kbajbekov'
yml = Path('/mnt/cephfs8_rw/nanopore2/test_space/results/7777/45gd/logs/slurm/slurm_tasks1.yaml')

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
    # Извлекаем данные о главной задаче
    task_data = get_user_jobs(user=usr).get(main_job_id, {})
    task_data['created'] = datetime.datetime.now()
    task_data['pid'] = proc_pid
    task_data['proc']  = proc
    task_data['child_jobs'] = {}
    print(task_data)

while True:
    time.sleep(5)
    # Читаем данные из squeue и трейса
    squeue_data = get_user_jobs(user=usr)
    if squeue_data:
        write_yaml(squeue_data, Path('/mnt/cephfs8_rw/nanopore2/test_space/results/7777/45gd/logs/slurm/slurm_squeue.yaml'))
    print(f"received squeue_data, keys:\n{'\n'.join([str(s) for s in squeue_data.keys()])}")
    # Проверяем, жив ли основной процесс; если нет, собираем exit_code и выходим
    task_data = update_task_data(task_data, squeue_data, True)
    # Добавляем данные о дочерних задачах
    task_data['child_jobs'] = get_child_jobs(squeue_data, main_job_id, task_data['child_jobs'])
    if task_data['exit_code'] is None:
        print(f'не выходим, exit_code: {task_data["exit_code"]}')
    else:
        print('пишем выходные данные, выходим')
        task_data.pop('proc')
        write_yaml(task_data, yml)
        exit()
    


    #['account', 'accrue_time', 'admin_comment', 'allocating_node', 'array_job_id', 'array_task_id', 'array_max_tasks', 'array_task_string', 'association_id', 'batch_features', 'batch_flag', 'batch_host', 'flags', 'burst_buffer', 'burst_buffer_state', 'cluster', 'cluster_features', 'command', 'comment', 'container', 'container_id', 'contiguous', 'core_spec', 'thread_spec', 'cores_per_socket', 'billable_tres', 'cpus_per_task', 'cpu_frequency_minimum', 'cpu_frequency_maximum', 'cpu_frequency_governor', 'cpus_per_tres', 'cron', 'deadline', 'delay_boot', 'dependency', 'derived_exit_code', 'eligible_time', 'end_time', 'excluded_nodes', 'exit_code', 'extra', 'failed_node', 'features', 'federation_origin', 'federation_siblings_active', 'federation_siblings_viable', 'gres_detail', 'group_id', 'group_name', 'het_job_id', 'het_job_id_set', 'het_job_offset', 'job_id', 'job_resources', 'job_size_str', 'job_state', 'last_sched_evaluation', 'licenses', 'licenses_allocated', 'mail_type', 'mail_user', 'max_cpus', 'max_nodes', 'mcs_label', 'memory_per_tres', 'name', 'network', 'nodes', 'nice', 'tasks_per_core', 'tasks_per_tres', 'tasks_per_node', 'tasks_per_socket', 'tasks_per_board', 'cpus', 'node_count', 'tasks', 'partition', 'prefer', 'memory_per_cpu', 'memory_per_node', 'minimum_cpus_per_node', 'minimum_tmp_disk_per_node', 'power', 'preempt_time', 'preemptable_time', 'pre_sus_time', 'hold', 'priority', 'priority_by_partition', 'profile', 'qos', 'reboot', 'required_nodes', 'required_switches', 'requeue', 'resize_time', 'restart_cnt', 'resv_name', 'scheduled_nodes', 'segment_size', 'selinux_context', 'shared', 'sockets_per_board', 'sockets_per_node', 'start_time', 'state_description', 'state_reason', 'standard_input', 'standard_output', 'standard_error', 'stdin_expanded', 'stdout_expanded', 'stderr_expanded', 'submit_time', 'suspend_time', 'system_comment', 'time_limit', 'time_minimum', 'threads_per_core', 'tres_bind', 'tres_freq', 'tres_per_job', 'tres_per_node', 'tres_per_socket', 'tres_per_task', 'tres_req_str', 'tres_alloc_str', 'user_id', 'user_name', 'maximum_switch_wait_time', 'wckey', 'current_working_directory']