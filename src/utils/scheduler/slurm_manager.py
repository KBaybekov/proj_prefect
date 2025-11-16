import time
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from ast import literal_eval 
from typing import Any, Dict, IO, List, Optional, Set, Union
from pathlib import Path
from pymongo.collection import Collection
from utils.logger import get_logger
from shlex import split as shlex_split
from slurm_task import SlurmTask
from json import loads as json_loads


logger = get_logger(__name__)

@dataclass(slots=True)
class SlurmManager:
    user:str
    poll_interval:int
    queue_size:int
    submitted_tasks_slurm:Dict[str, SlurmTask] = field(default_factory=dict)
    submitted_tasks_db:Dict[str, SlurmTask] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._check_slurm_presence()
        return

    def _check_slurm_presence(self) -> None:
        if self.run_subprocess(["which", "sbatch"]).returncode == 0:
            logger.debug("Slurm доступен.")
        else:
            logger.error("Slurm не доступен.")
            raise Exception("Slurm не доступен.")
    
    def _actualize_submitted_tasks_data(
                                        self,
                                        db_tasks:Dict[str, int]
                                       ) -> Dict[str, Any]:
        """
        Актуализирует данные о статусах задач в Slurm.
        Возвращает словарь с данными по задачам.
        """
        def __convert_to_slurm_task(
                                    tasks:Dict[str, Dict[str, Any]]
                                   ) -> Dict[str, SlurmTask]:
            
            return {
                    task_id: SlurmTask.from_dict(task_data)
                    for task_id, task_data in tasks.items()
                   }
        
        squeue_data = self._get_queued_tasks_data()
        # обновляем информацию о запущенных задачах
        if squeue_data:
            for job_id in db_tasks.values():
                
        # 


    def _get_queued_tasks_data(
                               self
                              ) -> Dict[int, Dict[str, Any]]:
        """
        Возвращает словарь job_id -> summary_dict, полученный из `squeue --json -u user`.
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
        rc, out = cmd_result.returncode, cmd_result.stdout
        if rc != 0:
            logger.error(f"Ошибка при запросе данных о задачах Slurm: exit_code {rc}")
            return {}
        # Преобразуем json-строку в словарь
        try:
            j = json_loads(out)
        except Exception as e:
            logger.error("Ошибка при преобразовании stdout squeue в json:", e)
            return {}
        # Проверяем наличие ключа "jobs", и если он есть, извлекаем его в виде списка
        jobs:List[Dict[Union[int, str], Any]] = j.get("jobs") or []
        result = {}
        # Проходим по всем задачам, извлекая нужные поля
        for job in jobs:
            job_id = job.get('job_id') 
            if not job_id:
                continue
            entry = {
                "job_id": job_id,
                "name": job.get('name', ''),
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
            # Для задач, которые ещё начали выполняться, извлекаем время старта и лимит
            for property, squeue_property in {'start':'start_time', 'limit':'end_time'}.items():
                time_val = None
                if entry['status'] == 'RUNNING':
                    time_val = __timestamp_to_datetime(job.get(squeue_property, {}).get('number'))
                entry.update({property: time_val})
            result[job_id] = entry
        return result
    
    def _update_task_data(
                          self,
                          task_id:str,
                         ) -> None:
        """
        Запрашивает в БД статус задачи. Предполагается, что в БД есть запись о задаче.
        Если статус не указывает на завершение ('CANCELLED', 'COMPLETED', 'FAILED'),
        обновляет статус путём запроса в Slurm по job_id. Если в Slurm нет указанного job_id,
        проверяет наличие выходных файлов.
        
        """
        def __is_task_completed(status:str) -> bool:
            if status in ['ok', 'failed']:
                return True
            else:
                return False
            
        task_completed = False
        task_record:SlurmTask = self.submitted_tasks.get(
                                                         task_id,
                                                         SlurmTask.from_db(self.dao.find_one(
                                                                                             collection="tasks",
                                                                                             query={"task_id": task_id},
                                                                                             projection={}
                                                                                            ))) # type: ignore
        # В случае, если статусы задачи в Slurm указывают на её завершение, направляем на процедуру завершения обработки задания
        task_completed = __is_task_completed(task_record.slurm_status)
        # В противном случае запрашиваем статус задачи в Slurm
        if task_completed:
            self._task_completion_check(task_record)
        else:
            task_record.slurm_status = self.slurm_manager._get_job_info(task_record.slurm_job_id)
            task_completed = __is_task_completed(task_record.slurm_status)
            if task_completed

            #!!! крч, тут у нас процедура обновления данных и завершения обработки, если задача всё.
            # У меня сейчас не хватает понимания, что там должно куда идти и откуда, так что я пока бросил всё это (30.10.25,20:30)
        



            
            #self._extract_task_results_to_sample(db_task_record)


    def submit_to_slurm(self, job_script: str, job_name: str, resources: Dict[str, Any]) -> str:
        """Отправляет задание в Slurm и возвращает ID задачи."""
        sbatch_cmd = ["sbatch"]
        for key, value in resources.items():
            sbatch_cmd.extend([f"--{key}", str(value)])
        sbatch_cmd.extend(["--job-name", job_name, job_script])

        try:
            result = subprocess.run(
                sbatch_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            job_id = result.stdout.strip().split()[-1]
            logger.info(f"Задание {job_name} отправлено в Slurm с ID {job_id}.")
            return job_id
        except subprocess.CalledProcessError as e:
            logger.error(f"Ошибка при отправке задания в Slurm: {e}")
            raise

    def _get_job_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Получает информацию о задании из Slurm."""
        try:
            result = subprocess.run(
                ["sacct", "-X", "-o", "JobID,State,ExitCode"],
                capture_output=True,
                text=True,
                check=True
            )
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

    def monitor_job(self, job_id: str, task_doc: Dict[str, Any]) -> None:
        """Мониторит статус задания и обновляет данные в БД."""
        while True:
            job_info = self.get_job_info(job_id)
            if not job_info:
                logger.warning(f"Задание {job_id} не найдено в Slurm.")
                self.handle_missing_job(task_doc)
                break
            elif job_info["state"] == "COMPLETED":
                self.update_task_status(job_id, "COMPLETED", None)
                break
            elif job_info["state"] == "FAILED":
                self.update_task_status(job_id, "FAILED", job_info["exit_code"])
                self.retry_or_notify(task_doc)
                break
            time.sleep(60)

    def handle_missing_job(self, task_doc: Dict[str, Any]) -> None:
        """Обрабатывает задачу, которая исчезла из Slurm."""
        if self.check_output_files(task_doc):
            logger.info(f"Задание {task_doc['job_id']} завершена успешно (проверка файлов).")
            self.update_task_status(task_doc["job_id"], "COMPLETED", None)
        else:
            logger.warning(f"Задание {task_doc['job_id']} не найдена и файлы отсутствуют.")
            self.update_task_status(task_doc["job_id"], "FAILED", "MissingOutputFiles")

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
                      ) -> op[subprocess.CompletedProcess]:
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
            logger.info(f"Запуск команды: {' '.join(cmd)}")

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
        if log_output and capture_output:
            logger.debug(f"STDOUT:\n{result.stdout}")
            logger.debug(f"STDERR:\n{result.stderr}")

        return result
