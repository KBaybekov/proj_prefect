import time
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, IO, List, Optional, Union
from pymongo.collection import Collection
from utils.logger import get_logger
from shlex import split as shlex_split


logger = get_logger(__name__)

class SlurmManager:
    def __init__(
                 self,
                 slurm_user: str
                ):
        self.user = slurm_user
        self._check_connection()
        return

    def _check_connection(self) -> None:
        if self.run_subprocess(["which", "sbatch"]).returncode == 0:
            logger.debug("Slurm доступен.")
        else:
            logger.error("Slurm не доступен.")
            raise Exception("Slurm не доступен.")
        
    def _get_queued_tasks_data(
                               self
                              ) -> Dict[str, Dict[str, Any]]:
        """
        Получает информацию о заданиях, находящихся в очереди Slurm.
        Возвращает словарь с информацией о заданиях.
        """
        data = self.run_subprocess(
                                   cmd=[
                                        "squeue",
                                        "--user", self.user,
                                        "json"
                                   ]
                                  )
    
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

    def run_subprocess(
                       self,
                       cmd: Union[str, List[str]],
                       check: bool = False,
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
                      ) -> subprocess.CompletedProcess:
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
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            raise

        # Логирование результата
        if log_output and capture_output:
            logger.debug(f"STDOUT:\n{result.stdout}")
            logger.debug(f"STDERR:\n{result.stderr}")

        return result