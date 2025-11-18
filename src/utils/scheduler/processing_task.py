from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import subprocess
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class TaskSlurmJob:
    # Идентификаторы
    job_id:int
    parent_job_id:int
    name:str
    # Отслеживание
    work_dir:Path
    partition:str
    priority:str
    nodes:str
    status:str
    stderr:Path
    stdout:Path
    exit_code:Optional[int]
    # Время
    start:Optional[datetime] = field(default=None)
    limit:Optional[datetime] = field(default=None)
    finish:Optional[datetime] = field(default=None)

    @staticmethod
    def from_dict(squeue_data:Dict[str, Any]) -> 'TaskSlurmJob':
        slurm_job = TaskSlurmJob(
                                 job_id=squeue_data['job_id'],
                                 parent_job_id=squeue_data['parent_job_id'],
                                 name=squeue_data['name'],
                                 work_dir=squeue_data['work_dir'],
                                 partition=squeue_data['partition'],
                                 priority=squeue_data['priority'],
                                 nodes=squeue_data['nodes'],
                                 status=squeue_data['status'],
                                 stderr=squeue_data['stderr'],
                                 stdout=squeue_data['stdout'],
                                 exit_code=squeue_data['exit_code'],
                                 start=squeue_data.get('start'),
                                 limit=squeue_data.get('limit')
                                )
        return slurm_job

    def _update(self, slurm_data:Dict[str, Any]):
        if slurm_data:
            self.status = slurm_data['status']

            # Если задача запущена, собираем данные по лимитам времени
            if self.status == 'RUNNING':
                for property in ['start', 'limit']:
                    if all([getattr(self, property) is None,
                            property in slurm_data]):
                        setattr(
                                self,
                                property,
                                slurm_data[property]
                               )
        
        else:
            self.finish = datetime.now(timezone.utc)
            self._collect_completed_process_data()

    def _complete(
                  self,
                  now:Optional[datetime] = None,
                  exit_code_f:Optional[Path] = None
                 ) -> None:
        """
        Завершение задачи.
        Отмечает время завершения.
        Получает exit-code.
        В зависимости от exit-code выставляет конечный статус задачи
        """
        if not now:
            now = datetime.now(timezone.utc)
        self.finish = now
        self._collect_completed_process_data(exit_code_f)

    def _collect_completed_process_data(
                                        self,
                                        exit_code_f:Optional[Path] = None
                                       ) -> None:
        if not exit_code_f:
            exit_code_f = (self.work_dir / '.exitcode').resolve()
        logger.debug(f"Проверяем {exit_code_f}")
        if exit_code_f.exists():
            logger.debug(f"Найден .exitcode в {self.work_dir.as_posix()}:")
            try:
                # Читаем первую строку и преобразуем в число
                with open(exit_code_f, 'r') as f:
                    self.exit_code = int(f.readline().strip())
                    logger.debug(f"exit_code: {self.exit_code}")
                return None
            except Exception as e:
                logger.error(f"Ошибка при чтении {exit_code_f.as_posix()}: {e}")
        else:
            logger.error(f"Не найден .exitcode в {self.work_dir.as_posix()}")
        
        self._define_task_status_by_exit_code()
        return None

    def _define_task_status_by_exit_code(
                                         self
                                        ) -> None:
        """
        Определение конечного статуса задачи Slurm по коду завершения
        """
        statuses = {
                    0: "COMPLETED",
                    124: "TIMEOUT"
                   }
        if self.exit_code is None:
            self.status = "UNKNOWN"
        elif isinstance(self.exit_code, int):
            self.status = statuses.get(self.exit_code, "FAILED")


@dataclass(slots=True)
class TaskData:
    # Словарь вида {тип файлов: множество путей к файлам}
    input_files:Dict[str,Set[Path]] = field(default_factory=dict)
    input_files_size:Dict[str,int] = field(default_factory=dict)
    # словарь вида {тип_файлов: маска}
    output_files_expected:Dict[str, str] = field(default_factory=dict)
    output_files:Dict[str,Set[Path]] = field(default_factory=dict)
    total_input_files_size:int = field(default=0)
    input_data_shaper:Path = field(default_factory=Path)
    output_data_shaper:Path = field(default_factory=Path)
    starting_script:Path = field(default_factory=Path)
    work_dir:Path = field(default_factory=Path)
    result_dir:Path = field(default_factory=Path)
    log_dir:Path = field(default_factory=Path)
    head_job_stdout:Path = field(default_factory=Path)
    head_job_stderr:Path = field(default_factory=Path)
    head_job_exitcode_f:Path = field(default_factory=Path)

    


    @staticmethod
    def from_dict(doc: Dict[str, Any]) -> 'TaskData':
        task_data = TaskData(
                             input_files={
                                          group_name:{Path(path) for path in paths}
                                          for group_name, paths in
                                          doc.get("input_files", {}).items()
                                         },
                             output_files={
                                           group_name:{Path(path) for path in paths}
                                           for group_name, paths in
                                           doc.get("output_files", {}).items()
                                          },
                             input_files_size=doc.get("input_files_size", {}),
                             output_files_expected=doc.get("output_files_expected", {}),
                             total_input_files_size=doc.get("total_input_files_size", 0),
                             input_data_shaper=Path(doc.get("input_data_shaper", "")),
                             output_data_shaper=Path(doc.get("output_data_shaper", "")),
                             starting_script=Path(doc.get("starting_script", "")),
                             work_dir=Path(doc.get("work_dir", "")),
                             result_dir=Path(doc.get("result_dir", "")),
                             log_dir=Path(doc.get("log_dir", "")),
                             head_job_stdout=Path(doc.get("head_job_stdout", "")),
                             head_job_stderr=Path(doc.get("head_job_stderr", "")),
                             head_job_exitcode_f=Path(doc.get("head_job_exitcode_f", ""))
                            )

        return task_data


@dataclass(slots=True)
class ProcessingTask:
    # Шаблон имени: sample_sample-fingerprint_pipeline-name_pipeline-version
    task_id:str
    sample:str = field(default_factory=str)
    pipeline_name:str = field(default_factory=str)
    sample_fingerprint:str = field(default_factory=str)
    pipeline_version:str = field(default_factory=str)
    # Файлы
    data: TaskData = field(default_factory=TaskData)
    # Время
    created:Optional[datetime] = field(default=None)
    queued:Optional[datetime] = field(default=None)
    finish:Optional[datetime] = field(default=None)
    last_update:Optional[datetime] = field(default=None)
    time_from_creation_to_finish:str = field(default_factory=str)
    time_in_processing:str = field(default_factory=str)
    # Slurm
    
    # Статусы:
    #  - "created" - задание только создано;
    #  - "prepared" - подготовлены все данные, необходимые для обработки;
    #  - "queued" - задание в очереди Slurm, обработка не начата;
    #  - "processing" - идёт обработка данных;
    #  - "completed" - задание успешно завершено;
    #  - "failed" - задание завершено с ошибкой.
    status:str = field(default="created")
    slurm_main_job:Optional[TaskSlurmJob] = field(default=None)
    slurm_child_jobs:Dict[int, TaskSlurmJob] = field(default_factory=dict)
    # Unix
    exit_code:Optional[int] = field(default=None)

    @staticmethod
    def from_dict(doc: Dict[str, Any]) -> 'ProcessingTask':
        """
        Создаёт объект Task из документа БД.

        :param doc: Документ из коллекции 'tasks' в MongoDB.
        :return: Объект Task.
        """
        # Инициализируем основные поля SampleMeta
        processing_task = ProcessingTask(
                               task_id=doc.get("task_id", ""),
                               sample=doc.get("sample", ""),
                               pipeline_name=doc.get("pipeline_name", ""),
                               sample_fingerprint=doc.get("sample_fingerprint", ""),
                               pipeline_version=doc.get("pipeline_version", ""),
                               data=TaskData.from_dict(doc.get("data", "")),
                               created=doc.get("created"),
                               queued=doc.get("queued"),
                               finish=doc.get("finish"),
                               last_update=doc.get("last_update"),
                               time_from_creation_to_finish=doc.get("time_from_creation_to_finish", ""),
                               time_in_processing=doc.get("time_in_processing", ""),
                               status=doc.get("status", ""),
                               slurm_main_job=TaskSlurmJob.from_dict(doc.get("slurm_main_job", {})),
                               slurm_child_jobs={
                                                 job_id:TaskSlurmJob.from_dict(job_data)
                                                 for job_id, job_data in
                                                 doc.get("slurm_child_jobs", {}).items()
                                                 },
                               exit_code=doc.get("exit_code")
                              )
        return processing_task
    
    def _put_in_queue(
                      self
                     ) -> None:
        """
        Перевод задания в состояние поставленного в очередь.
        """
        # Обновление меток времени
        now = self.__update_time_mark()
        self.queued = now
        self.time_in_processing = "00:00:00"
        self.status = "queued"
        # Создание необходимых директорий
        for dir_path in [
                         self.data.work_dir,
                         (self.data.log_dir / 'slurm').resolve()
                         ]:
            if not dir_path.exists():
                logger.debug(f"Создание директории {dir_path.as_posix()}")
                dir_path.mkdir(parents=True, exist_ok=True)

        self.slurm_main_job = TaskSlurmJob()

    def _update(
                self,
                slurm_data: Dict[str, Any]
               ) -> None:
        """
        Обновляет данные задания на основе данных Slurm, полученных через squeue.
        """
        # Обновление меток времени
        now = self.__update_time_mark()   
        self.time_in_processing = self._get_time_str((now - self.queued).total_seconds()) # type: ignore
        
        # Обновление данных заданий Slurm
        # обновляем данные главного задания
        if self.slurm_main_job:
            self.slurm_main_job._update(slurm_data)
        # Обновляем данные дочерних задач
        child_jobs = slurm_data.get("child_jobs", {})
        if child_jobs:
            for job_id, job_data in child_jobs.items():
                if job_id in self.slurm_child_jobs:
                    self.slurm_child_jobs[job_id]._update(job_data)
                else:
                    self.slurm_child_jobs[job_id] = TaskSlurmJob.from_dict(job_data)
        return None

    def _complete(
                  self
                 ) -> None:
        """
        Перевод задания в состояние завершённого.
        Сбор информации о выходных файлах.
        """
        # Обновление меток времени
        now = self.__update_time_mark()
        self.finish = now
        if self.slurm_main_job:
            self.slurm_main_job._complete(
                                          now,
                                          self.data.head_job_exitcode_f
                                         )

        # Поиск выходных файлов в папке результата
        logger.debug(f"Поиск выходных файлов в папке {self.data.result_dir.as_posix()}")
        if self.data.result_dir.exists():
            for file_group, file_mask in self.data.output_files_expected.items():
                # проводим поиск файлов по маске, добавляем только файлы с НЕНУЛЕВЫМИ размерами
                files = set([
                             f for f in self.data.result_dir.rglob(file_mask)
                             if all([
                                     f.is_file(),
                                     f.stat().st_size > 0
                                    ])
                           ])
                if files:
                    logger.debug(f"Найдено файлов группы {file_group}: {len(files)}")
                else:
                    logger.error(f"Файлы группы не {file_group} найдены")
                self.data.output_files[file_group] = files
        else:
            logger.error(f"Папка результата {self.data.result_dir.as_posix()} не найдена.")
        
        return None


    def __update_time_mark(
                           self
                          ) -> datetime:
        """
        Обновляет метку времени последнего обновления.
        Возвращает текущее время.
        """
        now = datetime.now(timezone.utc)
        self.last_update = now
        # Обновляем общее время, которое считаем с момента создания задания
        self.time_from_creation_to_finish = self._get_time_str((now - self.created).total_seconds()) # type: ignore
        return now

    def _get_time_str(
                      self,
                      seconds:float = 0.0
                     ) -> str:
        """
        Конвертирует секунды в строку формата "HH:MM:SS".
        """
        if all([
                seconds,
                seconds >= 1
               ]):
            hours, remainder = divmod(int(seconds), 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return "00:00:00"


    def to_db(self) -> None:
        return