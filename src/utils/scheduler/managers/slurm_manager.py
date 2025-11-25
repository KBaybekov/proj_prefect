import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, IO, List, Optional, Union
from pathlib import Path
from utils.logger import get_logger
from shlex import split as shlex_split
from classes.processing_task import ProcessingTask
from json import loads as json_loads


logger = get_logger(__name__)

@dataclass(slots=True)
class SlurmManager:
    _cfg:Dict[str, Any]
    user:str = field(default_factory=str)
    poll_interval:int = field(default=5)
    # Максимальное количество головных заданий 
    ljf_queue_size:int = field(default=14)
    sjf_queue_size:int = field(default=2)
    # словарь {job_id:job_squeue_data}
    squeue_data:Dict[
                     int,
                     Dict[
                          str,
                          Union[
                                datetime,
                                dict,
                                int,
                                None,
                                Path,
                                str
                    ]]] = field(default_factory=dict)


    def __post_init__(self) -> None:
        self.user = self._cfg.get('user', "")
        if not self.user:
            logger.error(f"Пользователь Slurm не указан в конфигурации!")
        self.ljf_queue_size = self._cfg.get('ljf_queue_size', 2)
        self.sjf_queue_size = self._cfg.get('sjf_queue_size', 14)
        self._check_slurm_presence()
        return

    def _check_slurm_presence(self) -> None:
        """
        Проверяет доступность Slurm.
        """
        result =  self._run_subprocess(["which", "sbatch"])
        if result:
            if result.returncode == 0:
                logger.debug("Slurm доступен.")
                return None
        logger.error("Slurm не доступен.", )
        raise Exception("Slurm не доступен.")
    
    def _get_queued_tasks_data(
                               self
                              ) -> None:
        """
        Возвращает словарь job_id -> summary_dict, полученный из `squeue --json -u user`.
        Также добавляет вложенный словарь дочерних задач к основным задачам.
        Не вызывает scontrol (чтобы быть лёгким).
        """
        def __timestamp_to_datetime(timestamp: Optional[int]) -> Optional[datetime]:
            """
            Преобразование timestamp (число секунд) в datetime.
            """
            if timestamp:
                return datetime.fromtimestamp(timestamp)
            return None

        # Получаем данные о задачах, созданных пользователем, в виде json-строки
        cmd_result = self._run_subprocess([
                                           "squeue",
                                           "--json",
                                           "-u",
                                           self.user],
                                          timeout=15,
                                          critical=True
                                         )
        if cmd_result:
            rc, out = cmd_result.returncode, cmd_result.stdout
            if rc != 0:
                logger.error(f"Ошибка при запросе данных о задачах Slurm: exit_code {rc}")
                return None
            # Преобразуем json-строку в словарь
            try:
                j = json_loads(out)
            except Exception as e:
                logger.error("Ошибка при преобразовании stdout squeue в json:", e)
                return None
        # Проверяем наличие ключа "jobs", и если он есть, извлекаем его в виде списка
        jobs:List[Dict[Union[int, str], Any]] = j.get("jobs") or [] # type: ignore
        # Проходим по всем задачам, извлекая нужные поля
        for job in jobs:
            job_id = job.get('job_id') 
            if not job_id:
                continue
            entry = {
                     "job_id": job_id,
                     "name": job.get('name', ''),
                     "array_job_id": job.get('array_job_id', {}).get('number', 0),
                     "parent_job_id": 0
                                       if not job.get('comment', '')
                                       else int(job.get('comment', '').removeprefix("parent_job_id ")),
                     "nodes": job.get('nodes', "").split(','),
                     "partition": job.get('partition'),
                     "priority": job.get('priority', {}).get('number', 0),
                     "status": job.get('job_state', [])[-1],
                     "stderr": '' 
                                 if job.get('standard_error', '') == '/dev/null'
                                 else Path(job.get('standard_error', '')),
                     "stdout": '' 
                                 if job.get('standard_output', '') == '/dev/null'
                                 else Path(job.get('standard_output', '')),
                     "exit_code": job.get('exit_code', {}).get('number'),
                     "work_dir": Path(job.get('current_working_directory', ''))
                    }
            # Для задач, которые ещё начали выполняться, извлекаем время старта и лимит
            for property, squeue_property in {
                                              'start':'start_time',
                                              'limit':'end_time'
                                             }.items():
                time_val = None
                if entry['status'] == 'RUNNING':
                    time_val = __timestamp_to_datetime(job.get(squeue_property, {}).get('number'))
                entry.update({property: time_val})
             
            self.squeue_data[job_id] = entry
        
        # Добавляем дочерние задачи
        for job_id, entry in self.squeue_data.items():
            parent_job_entry = self.squeue_data.get(entry['parent_job_id']) # type: ignore
            if parent_job_entry:
                if 'child_jobs' not in parent_job_entry:
                    parent_job_entry['child_jobs'] = {}
                parent_job_entry['child_jobs'].update({job_id:entry}) # type: ignore

        return None
    
    def _submit_to_slurm(
                         self,
                         task:ProcessingTask
                        ) -> None:
        """Отправляет задание в Slurm"""
        def create_necessary_dirs():
            # Создание необходимых директорий
            for dir_path in [
                             task.data.work_dir,
                             (task.data.log_dir / 'slurm').resolve()
                            ]:
                if not dir_path.exists():
                    logger.debug(f"Создание директории {dir_path.as_posix()}")
                    dir_path.mkdir(parents=True, exist_ok=True)

        def extract_slurm_job_id(stdout: str) -> int:
            """
            Извлекает Slurm ID задачи из stdout
            """
            job_id = 0
            try:
                for line in stdout.splitlines():
                    line = line.strip()
                    if not line:
                            continue
                    if line.startswith("Submitted batch job"):
                        parts = line.split()
                        if len(parts) == 4 and parts[3].isdigit():
                            job_id = int(parts[3])
                            return job_id
            except Exception as e:
                    logger.error(f"Ошибка при чтении stdout: {e}")
            finally:
                return job_id

        sbatch_cmd = [
                      'bash',
                      task.data.start_script.as_posix()  
                     ]
        try:
            create_necessary_dirs()
            result = self._run_subprocess(
                                          cmd=sbatch_cmd,
                                          check=True,
                                          capture_output=True,
                                         )
            if result:
                if result.returncode == 0:
                    job_id = extract_slurm_job_id(result.stdout)
                    if job_id:
                        task._now_in_queue(job_id)
                        logger.info(f"Задание {task.task_id} отправлено в Slurm с ID {job_id}.")
                        return None
                else:
                    logger.error(f"Ошибка при отправке задания {task.task_id} в Slurm: {result.stderr}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка при отправке задания в Slurm: {e}")
        task.status = 'failed'
        return None

    def _get_job_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Получает информацию о задании из Slurm."""
        try:
            result = self._run_subprocess(
                ["sacct", "-X", "-o", "JobID,State,ExitCode"],
                capture_output=True,
                text=True,
                check=True
            )
            if result:
                lines = result.stdout.strip().split("\n")[1:]  # Пропускаем заголовок
                for line in lines:
                    parts = line.split()
                    if parts[0] == job_id:
                        return {
                            "job_id": job_id,
                            "state": parts[1],
                            "exit_code": parts[2] if len(parts) > 2 else None
                        }
                return None
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка при получении информации о задании {job_id}: {e}")
            return None

    def _run_subprocess(
                       self,
                       cmd: Union[str, List[str]],
                       check: bool = False,
                       critical: bool = False,
                       timeout: Optional[float] = None,
                       capture_output: bool = False,
                       env: Optional[Dict[str, str]] = None,
                       cwd: Optional[str] = None,
                       stdin: Optional[Union[IO, int]] = None,
                       stdout: Optional[Union[IO, int]] = None,
                       stderr: Optional[Union[IO, int]] = None,
                       input: Optional[str] = None,
                       text: bool = True,
                       shell: bool = False,
                       log_output: bool = True,
                       suppress_output: bool = False,
                      ) -> Optional[subprocess.CompletedProcess]:
        """
        Обёртка над subprocess.run с расширенной логикой и безопасностью.

        :param cmd: Команда как строка или список аргументов.
        :param check: Если True, вызывает CalledProcessError при неудачном коде возврата.
        :param timeout: Таймаут выполнения в секундах.
        :param capture_output: Ловить ли stdout и stderr.
        :param env: Словарь переменных окружения.
        :param cwd: Рабочая директория.
        :param stdin: Входной поток.
        :param stdout: Выходной поток.
        :param stderr: Поток ошибок.
        :param input: Входные данные для stdin.
        :param text: Использовать текстовый режим.
        :param shell: Использовать shell (опасно!).
        :param log_output: Логировать вывод команды.
        :param suppress_output: Подавлять вывод в консоль.
        :return: Объект subprocess.CompletedProcess.
        """
        result = None
        # Преобразование строки в список (если shell=False)
        if isinstance(cmd, str) and not shell:
            try:
                cmd = shlex_split(
                                  cmd,
                                  comments=False,
                                  posix=True
                                 )
            except ValueError as e:
                logger.error(f"Ошибка разбора команды '{cmd}': {e}")
                raise

        # Логирование команды
        if log_output:
            logger.debug(f"Запуск команды: {' '.join(cmd)}")

        # Настройка перенаправления вывода
        if suppress_output:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        elif capture_output:
            stdout = subprocess.PIPE
            stderr = subprocess.PIPE

        try:
            result = subprocess.run(
                                    cmd,
                                    check=check,
                                    timeout=timeout,
                                    env=env,
                                    cwd=cwd,
                                    stdin=stdin,
                                    stdout=stdout,
                                    stderr=stderr,
                                    input=input,
                                    text=text,
                                    shell=shell,
                                   )
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка выполнения команды: {' '.join(e.cmd)}")
            logger.error(f"Код возврата: {e.returncode}")
            if e.stdout:
                logger.error("STDOUT:")
                logger.error(e.stdout)
            if e.stderr:
                logger.error("STDERR:")
                logger.error(e.stderr)
            raise
        except subprocess.TimeoutExpired as e:
            logger.error(f"Таймаут команды: {' '.join(e.cmd)}")
            logger.error(f"Время ожидания: {timeout}s")
            if critical:
                raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            if critical:
                raise

        # Логирование результата
        if result:
            if log_output and capture_output:
                logger.debug(f"STDOUT:\n{result.stdout}")
                logger.debug(f"STDERR:\n{result.stderr}")

        return result
