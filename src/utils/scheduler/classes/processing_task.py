from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Callable, Optional, Set
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
            self._complete()

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
    # Словарь вида {группа файлов: {тип файлов: множество путей к файлам}}
    input_data:Dict[str, Dict[str,Set[Path]]] = field(default_factory=dict)
    input_files_size:int = field(default=0)
    # словарь вида {группа(например, QC): {тип_файлов: маска для поиска}}
    expected_output_data:Dict[str, Dict[str,str]] = field(default_factory=dict)
    # словарь вида {группа(например, QC): {тип_файлов: множество путей к файлам}}
    output_data:Dict[str, Dict[str,Set[Path]]] = field(default_factory=dict)
    output_files_size:int = field(default=0)
    input_data_shaper:Optional[Callable] = field(default=None)
    output_data_shaper:Optional[Callable] = field(default=None)
    starting_script:Path = field(default_factory=Path)
    work_dir:Path = field(default_factory=Path)
    result_dir:Path = field(default_factory=Path)
    log_dir:Path = field(default_factory=Path)
    head_job_stdout:Path = field(default_factory=Path)
    head_job_stderr:Path = field(default_factory=Path)
    head_job_exitcode_f:Path = field(default_factory=Path)

    


    @staticmethod
    def from_dict(doc: Dict[str, Any]) -> 'TaskData':
        def __extract_data_from_doc(
                                    item: Dict[str, Any]
                                   ) -> Dict[str, Dict[str,Set[Path]]]:
            extracted_data = {}
            for group_name, group_data in item.items():
                unpacked_data = {}
                for filetype, files in group_data.items():
                    unpacked_data[filetype] = set([Path(f) for f in files])
                extracted_data[group_name] = unpacked_data
            return extracted_data

        task_data = TaskData(
                             input_data=__extract_data_from_doc(doc.get("input_data", {})),
                             expected_output_data=doc.get("expected_output_data", {}),
                             output_data=__extract_data_from_doc(doc.get("output_data", {})),
                             input_files_size=doc.get("input_files_size", 0),
                             output_files_size=doc.get("output_files_size", 0),
                             starting_script=Path(doc.get("starting_script", "")),
                             work_dir=Path(doc.get("work_dir", "")),
                             result_dir=Path(doc.get("result_dir", "")),
                             log_dir=Path(doc.get("log_dir", "")),
                             head_job_stdout=Path(doc.get("head_job_stdout", "")),
                             head_job_stderr=Path(doc.get("head_job_stderr", "")),
                             head_job_exitcode_f=Path(doc.get("head_job_exitcode_f", ""))
                            )

        return task_data
    
    def _check_input_files(
                           self
                          ) -> None:

    def _check_output_files(
                            self
                           ) -> None:
        """
        Осуществляет поиск ожидаемых выходных файлов по маскам в self.expected_output_data.
        Сохраняет результаты в self.output_data.
        """
        logger.debug(f"Поиск выходных файлов в папке {self.result_dir.as_posix()}")
        if self.result_dir.exists():
            for file_group, filetypes in self.expected_output_data.items():
                self.output_data[file_group] = {}
                for filetype, file_mask in filetypes.items():
                    # проводим поиск файлов по маске, добавляем только файлы с НЕНУЛЕВЫМИ размерами
                    files = set([
                                f for f in self.result_dir.rglob(file_mask)
                                if all([
                                        f.is_file(),
                                        f.stat().st_size > 0
                                       ])
                                ])
                    if files:
                        logger.debug(f"Найдено файлов группы {file_group}: {len(files)}")
                    else:
                        logger.error(f"Файлы группы не {file_group} найдены")
                    self.output_data[file_group].update({filetype:files})
        else:
            logger.error(f"Папка результата {self.result_dir.as_posix()} не найдена.")
        return None


@dataclass(slots=True)
class ProcessingTask:
    # Шаблон имени: sample_sample-fingerprint_pipeline-name_pipeline-version
    task_id:str
    sample:str = field(default_factory=str)
    sample_fingerprint:str = field(default_factory=str)
    sample_id:str = field(default_factory=str)
    pipeline_name:str = field(default_factory=str)
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
                                         sample_fingerprint=doc.get("sample_fingerprint", ""),
                                         sample_id=doc.get("sample_id", ""),
                                         pipeline_name=doc.get("pipeline_name", ""),
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
    
    def __post_init__(
                      self
                     ) -> None:
        self.sample, self.sample_fingerprint, self.pipeline_name, self.pipeline_version = self.task_id.split("_")
        self.sample_id = f"{self.sample}_{self.sample_fingerprint}"
        

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
        self.data._check_output_files()        
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