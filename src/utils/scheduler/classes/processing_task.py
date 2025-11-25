from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from os import access as os_access, W_OK, R_OK
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, Set, Union
import subprocess
from scheduler import *
from tools.script_renderer import ScriptRenderer
from .pipeline import Pipeline
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class TaskSlurmJob:
    # Идентификаторы
    job_id:int
    parent_job_id:int = field(default=0)
    name:Optional[str] = field(default=None)
    # Отслеживание
    work_dir:Optional[Path] = field(default=None)
    partition:Optional[str] = field(default=None)
    priority:Optional[str] = field(default=None)
    nodes:Optional[str] = field(default=None)
    status:Optional[str] = field(default=None)
    stderr:Optional[Path] = field(default=None)
    stdout:Optional[Path] = field(default=None)
    exit_code:Optional[int] = field(default=None)
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
            for attr, value in self.__dict__.items():
                if attr in ['job_id', 'start', 'limit']: continue
                setattr(self, attr, slurm_data.get(attr, value))
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
        return None

    def _collect_completed_process_data(
                                        self,
                                        exit_code_f:Optional[Path] = None
                                       ) -> None:
        if self.work_dir:
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
    # Словарь вида {группа файлов: множество путей к файлам}}
    input_data:Dict[str, Set[Path]] = field(default_factory=dict)
    input_files_size:int = field(default=0)
    # словарь вида {группа(например, QC): {тип_файлов: маска для поиска}}
    expected_output_data:Dict[str, Dict[str,str]] = field(default_factory=dict)
    # словарь вида {группа(например, QC): {тип_файлов: множество путей к файлам}}
    output_data:Dict[str, Dict[str,Set[Path]]] = field(default_factory=dict)
    output_files_size:int = field(default=0)
    start_script:Path = field(default_factory=Path)
    work_dir:Path = field(default_factory=Path)
    result_dir:Path = field(default_factory=Path)
    log_dir:Path = field(default_factory=Path)
    head_job_stdout:Path = field(default_factory=Path) 
    head_job_stderr:Path = field(default_factory=Path)
    head_job_exitcode_f:Path = field(default_factory=Path)

    @staticmethod
    def from_dict(doc: Dict[str, Any]) -> 'TaskData':
        def __extract_input_data_from_doc(
                                          item: Dict[str, Any]
                                         ) -> Dict[str, Set[Path]]:
            extracted_data = {}
            for group_name, files in item.items():
                extracted_data[group_name] = set([Path(f) for f in files])
            return extracted_data

        def __extract_output_data_from_doc(
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
                             input_data=__extract_input_data_from_doc(doc.get("input_data", {})),
                             expected_output_data=doc.get("expected_output_data", {}),
                             output_data=__extract_output_data_from_doc(doc.get("output_data", {})),
                             input_files_size=doc.get("input_files_size", 0),
                             output_files_size=doc.get("output_files_size", 0),
                             start_script=Path(doc.get("start_script", "")),
                             work_dir=Path(doc.get("work_dir", "")),
                             result_dir=Path(doc.get("result_dir", "")),
                             log_dir=Path(doc.get("log_dir", "")),
                             head_job_stdout=Path(doc.get("head_job_stdout", "")),
                             head_job_stderr=Path(doc.get("head_job_stderr", "")),
                             head_job_exitcode_f=Path(doc.get("head_job_exitcode_f", ""))
                            )

        return task_data

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
    sample_meta:SampleMeta
    result_meta:ResultMeta
    pipeline:Pipeline
    # Индикаторы
    # Шаблон имени: sample_id_pipeline-name_pipeline-version
    task_id:str = field(default_factory=str)
    status:str = field(default="created")
    sorting:str = field(default_factory=str)
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
    #  - "disprepared" - подготовка прошла неудачно;
    #  - "queued" - задание в очереди Slurm, обработка не начата;
    #  - "processing" - идёт обработка данных;
    #  - "completed" - задание успешно завершено;
    #  - "failed" - задание завершено с ошибкой.
    slurm_main_job:Optional[TaskSlurmJob] = field(default=None)
    slurm_child_jobs:Dict[int, TaskSlurmJob] = field(default_factory=dict)
    # Unix
    exit_code:Optional[int] = field(default=None)

    @staticmethod
    def from_dict(doc: Dict[str, Any]) -> 'ProcessingTask':
        """
        Создаёт объект ProcessingTask из документа БД.

        :param doc: Документ из коллекции 'tasks' в MongoDB.
        :return: Объект ProcessingTask.
        """
        # Инициализируем основные поля SampleMeta
        return ProcessingTask(
                              task_id=doc.get("task_id", ""),
                              sample_meta=SampleMeta.from_dict(doc.get("sample_meta", "")),
                              result_meta=ResultMeta.from_dict(doc.get("result_meta", "")),
                              pipeline=Pipeline.from_dict(doc.get("pipeline", "")),
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
    
    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Конвертирует объект ProcessingTask в словарь для сохранения в БД.
        """
        return {
                "task_id": self.task_id,
                "sample_meta": self.sample_meta.to_dict(),
                
                
               }
    
    def __post_init__(
                      self
                     ) -> None:
        self.task_id = f"{self.sample_meta.sample_id}_{self.pipeline.id}"
        self.sorting = self.pipeline.sorting

    def _prepare_data(
                      self,
                      script_renderer:ScriptRenderer,
                      head_job_node:str                      
                     ) -> None:
        """
        Использует метаданные SampleMeta и ResultMeta для подготовки данных для обработки по инструкциям, указанным в Pipeline.
        Возвращает True, если подготовка прошла успешно, и False в противном случае.
        """
        def __check_n_group_input_data(
                                       self:ProcessingTask,
                                       script_renderer:ScriptRenderer,
                                       head_job_node:str
                                      ) -> bool:
            """
            Запускает шейпер входных данных.
            Валидирует данные и добавляет их в задание, если они валидны.
            """
            def __generate_start_script(nxf_cmd:str) -> Path:
                start_script = script_renderer.render_starting_script(
                                                                      script_filename=f"start_{self.task_id}.sh",
                                                                      job_name=self.task_id,
                                                                      work_dir=self.data.work_dir,
                                                                      res_dir=self.data.result_dir,
                                                                      log_dir=self.data.log_dir,
                                                                      pipeline_timeout=self.pipeline.timeout,
                                                                      nxf_command=nxf_cmd,
                                                                      head_job_node=head_job_node,
                                                                      head_job_stdout=self.data.head_job_stdout,
                                                                      head_job_stderr=self.data.head_job_stderr,
                                                                      head_job_exitcode_f=self.data.head_job_exitcode_f,
                                                                      environment_variables=self.pipeline.environment_variables,
                                                                      nxf_variables=self.pipeline.nextflow_variables,
                                                                      slurm_options=self.pipeline.slurm_options
                                                                      )
                return start_script

            if not self.pipeline.shape_input:
                logger.error("Не задана функция shape_input в pipeline.")
                return False
            try:
                # Вызываем shape_input из pipeline
                # Ожидается, что она вернёт команду Nextflow, путь к TSV и словарь входных данных (включает элемент {'size':int})
                shaper_data:Tuple[
                                  str,
                                  Dict[
                                       str,
                                       Union[Set[Path], int]
                                      ],
                                  Dict[
                                       str,
                                       Dict[str,str]
                                      ]
                                 ] = self.pipeline.shape_input(self)
                nxf_cmd, input_data, self.data.expected_output_data = shaper_data
                # Проверяем валидность данных
                if all([
                        len(shaper_data) == 3,
                        nxf_cmd,
                        self.data.expected_output_data,
                        isinstance(nxf_cmd, str),
                        isinstance(input_data, dict),
                        isinstance(self.data.expected_output_data, dict)
                      ]):
                    # Заполняем информацию о входных данных
                    self.data.input_files_size = input_data.pop('size') # type: ignore
                    self.data.input_data = input_data  # type: ignore
                    if all([
                            self.data.input_files_size > 0,
                            self.data.input_data
                          ]):
                        # Генерируем старт-скрипт
                        self.data.start_script = __generate_start_script(nxf_cmd)
                        if self.data.start_script:
                            return True
                        else:
                            logger.error("Файл скрипта не создан")
                            return False
                    else:
                        logger.error("Шейпер вернул пустые входные данные.")
                        return False
                else:
                    logger.error("Шейпер входных данных вернул не все данные.")
                    return False
                
            except Exception as e:
                logger.error(f"Ошибка при вызове shape_input: {e}")
                return False            

        def __form_task_directories(
                                    self:ProcessingTask
                                   ) -> bool:
            pipepline_subdirs = Path(self.pipeline.name) / Path(self.pipeline.version.replace('.', ''))
            self.data.work_dir = (self.sample_meta.processing_dir / pipepline_subdirs).resolve()
            self.data.result_dir = (self.sample_meta.result_dir / pipepline_subdirs).resolve()
            self.data.log_dir = (self.data.result_dir / 'logs').resolve()
            formed_directories = [self.data.work_dir, self.data.result_dir, self.data.log_dir]
            if not all(formed_directories):
                logger.error(f"""Не сформированы пути директории {self.task_id}:
                             \n\twork_dir:{self.data.work_dir.as_posix()}
                             \n\tresult_dir:{self.data.result_dir.as_posix()}
                             \n\tlog_dir:{self.data.log_dir.as_posix()}""".replace('  ', ''))
                return False
            for dir_path in formed_directories:
                if not dir_path.exists():
                    logger.debug(f"Директория {dir_path.as_posix()} не существует. Создание...")
                    dir_path.mkdir(parents=True, exist_ok=True)
                if dir_path.exists():
                    # Проверяем, что директория доступна для записи и чтения
                    if os_access(dir_path, W_OK & R_OK):
                        logger.debug(f"Директория {dir_path.as_posix()} доступна для чтения и записи.")
                    else:
                        logger.error(f"Директория {dir_path.as_posix()} не доступна для чтения/записи.")
                        return False
            return True
        
        def __form_head_job_pipe_files_paths(
                                             self:ProcessingTask
                                            ) -> bool:
            """
            Формирует пути для файлов stdout/stderr/exitcode головной задачи Slurm без их создания.
            Возвращает True, если пути сформированы успешно, и False в противном случае.
            """
            try:
                self.data.head_job_stdout = (self.data.log_dir / "slurm" / f"{self.task_id}.stdout").resolve()
                self.data.head_job_stderr = (self.data.log_dir / "slurm" / f"{self.task_id}.stderr").resolve()
                self.data.head_job_exitcode_f = (self.data.work_dir / f"{self.task_id}.exitcode").resolve()
                return True
            except Exception as e:
                logger.error(f"Ошибка при формировании путей для файлов stdout/stderr/exitcode головной задачи Slurm для задания {self.task_id}: {e}")
                return False

        task_directories_prepared = False
        input_data_ready_n_starting_script_created = False
        head_job_pipe_files_paths_present = False
        # Формируем основные собственные атрибуты
        # Обновление меток времени
        self.created = self.__update_time_mark()
        # формируем пути для обработки и хранения данных
        task_directories_prepared = __form_task_directories(self)
        if task_directories_prepared:
            # Формируем пути для будущих stdout/stderr/exitcode головной задачи Slurm
            head_job_pipe_files_paths_present = __form_head_job_pipe_files_paths(self)
            if head_job_pipe_files_paths_present:
                # Формируем группы входных данных
                input_data_ready_n_starting_script_created = __check_n_group_input_data(
                                                                                        self,
                                                                                        script_renderer,
                                                                                        head_job_node
                                                                                       )
        # Проверяем, что все данные готовы
        data_is_ok = all([
                          task_directories_prepared,
                          head_job_pipe_files_paths_present,
                          input_data_ready_n_starting_script_created
                         ])
        if data_is_ok:
            self.status = "prepared"
            logger.debug(f"Подготовка данных задания {self.task_id} успешно завершена.")
        else:
            self.status = "disprepared"
            logger.error(f"Не удалось подготовить данные задания {self.task_id}.")
        return None

    def _now_in_queue(
                      self,
                      job_id:int
                     ) -> None:
        """
        Перевод задания в состояние поставленного в очередь.
        """
        # Обновление меток времени
        now = self.__update_time_mark()
        self.queued = now
        self.time_in_processing = "00:00:00"
        self.status = "queued"
        self.slurm_main_job = TaskSlurmJob(job_id)
        return None

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
        Обновление статуса в соответствии со статусом главной задачи Slurm.
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
            self.status = 'completed' if self.slurm_main_job.status == 'completed' else 'failed'
        # Поиск выходных файлов в папке результата
        self.data._check_output_files()
        # Добавляем информацию о выходных файлах в ResultMeta
        self.result_meta.results[self.pipeline.id] = self.data.output_data
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
