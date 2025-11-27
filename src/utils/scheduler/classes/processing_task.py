from dataclasses import dataclass, field
from datetime import datetime, timezone
from os import access as os_access, W_OK, R_OK
from pathlib import Path
from typing import Any, Dict, Tuple, Optional, Set, Union
from scheduler import *
from tools.script_renderer import ScriptRenderer
from . import *
from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass(slots=True)
class ProcessingTask:
    """
    Класс для управления задачей обработки образца с помощью заданного пайплайна.

    Хранит метаданные образца и результатов, информацию о пайплайне,
    состояние выполнения, данные для запуска в Slurm и временные метки.

    Статусы:
     - "created" - задание только создано;
     - "prepared" - подготовлены все данные, необходимые для обработки;
     - "disprepared" - подготовка прошла неудачно;
     - "queued" - задание в очереди Slurm, обработка не начата;
     - "processing" - идёт обработка данных;
     - "completed" - задание успешно завершено;
     - "failed" - задание завершено с ошибкой.
    """
    sample_meta:SampleMeta
    """Метаданные образца, подлежащего обработке."""

    result_meta:ResultMeta
    """Метаданные результатов обработки образца."""
    
    pipeline:Pipeline
    """Пайплайн, используемый для обработки."""

    # Индикаторы
    task_id:str = field(default_factory=str)
    """
    Уникальный идентификатор задания.
    Формируется автоматически как 'sample_id_pipeline-id' в post_init.
    """

    status:str = field(default="created")
    """Текущий статус задания."""

    sorting:str = field(default_factory=str)
    """Алгоритм сортировки задания (например, 'SJF', 'LJF')."""
    # Файлы

    data: TaskData = field(default_factory=TaskData)
    """Данные, необходимые для выполнения задания (пути, скрипты, файлы)."""
    # Время

    created:Optional[datetime] = field(default=None)
    """Время создания задания."""

    queued:Optional[datetime] = field(default=None)
    """Время постановки задания в очередь Slurm."""

    finish:Optional[datetime] = field(default=None)
    """Время завершения обработки задания."""

    last_update:Optional[datetime] = field(default=None)
    """Время последнего обновления состояния задания."""

    time_from_creation_to_finish:str = field(default_factory=str)
    """Общее время выполнения задания в формате 'HH:MM:SS'."""

    time_in_processing:str = field(default_factory=str)
    """Время в обработке (с момента постановки в очередь) в формате 'HH:MM:SS'."""

    # Slurm
    slurm_main_job:Optional[TaskSlurmJob] = field(default=None)
    """Информация о главной задаче Slurm."""

    slurm_child_jobs:Dict[int, TaskSlurmJob] = field(default_factory=dict)
    """Информация о дочерних задачах Slurm (если есть)."""

    exit_code:Optional[int] = field(default=None)
    """Код завершения главной задачи Slurm."""

    @staticmethod
    def from_dict(
                  doc: Dict[str, Any],
                  result_meta: Dict[str, Any],
                  sample_meta: Dict[str, Any]
                 ) -> 'ProcessingTask':
        """
        Создаёт объект ProcessingTask из документа MongoDB.

        :param doc: Документ из коллекции 'tasks'.
        :param result_meta: Документ из коллекции 'results'.
        :param sample_meta: Документ из коллекции 'samples'.
        :return: Экземпляр ProcessingTask.
        """
        # Инициализируем основные поля SampleMeta
        return ProcessingTask(
                              task_id=doc.get("task_id", ""),
                              # SampleMeta и ResultMeta запрашиваются из БД отдельно
                              sample_meta=SampleMeta.from_dict(sample_meta),
                              result_meta=ResultMeta.from_dict(result_meta),
                              pipeline=Pipeline.from_dict(doc.get("pipeline", {})),
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
        Конвертирует объект в словарь для сохранения в MongoDB.

        Исключает приватные атрибуты и заменяет объекты метаданных
        на их идентификаторы.

        :return: Словарь с данными задания.
        """
        keys2remove = []
        dict_obj = self.__dict__
        for key in dict_obj:
            
            if key.startswith("_"):
                keys2remove.append(key)

            if key in ["sample_meta", "result_meta"]:
                if dict_obj[key]:
                    dict_obj[key] = dict_obj[key].sample_id
                else:
                    dict_obj[key] = ""
            
            if key in ['data', 'slurm_main_job']:
                if dict_obj[key]:
                    dict_obj[key] = dict_obj[key].to_dict()
                else:
                    dict_obj[key] = {}

            if key == 'slurm_child_jobs':
                if dict_obj[key]:
                    dict_obj[key] = {
                                     job_id:val.to_dict()
                                     for job_id, val in dict_obj[key].items()
                                    }
                else:
                    dict_obj[key] = {}
            
        for key in keys2remove:
            del dict_obj[key]
                    
        return dict_obj
    
    def __post_init__(
                      self
                     ) -> None:
        """
        Автоматически заполняет поля task_id и sorting после инициализации.
        Выполняется автоматически при создании экземпляра.
        """
        self.task_id = f"{self.sample_meta.sample_id}_{self.pipeline.id}"
        self.sorting = self.pipeline.sorting

    def _prepare_data(
                      self,
                      script_renderer:ScriptRenderer,
                      head_job_node:str                      
                     ) -> None:
        """
        Подготавливает все необходимые данные для выполнения задания.

        Включает:
         - создание директорий для обработки и результатов;
         - формирование путей к служебным файлам Slurm;
         - вызов шейпера входных данных;
         - генерацию стартового скрипта.

        При успешной подготовке статус меняется на 'prepared',
        при неудаче — на 'disprepared'.

        :param script_renderer: Объект для рендеринга стартовых скриптов.
        :param head_job_node: Имя ноды для запуска головной задачи Slurm.
        """

        def __check_n_group_input_data(
                                       self:ProcessingTask,
                                       script_renderer:ScriptRenderer,
                                       head_job_node:str
                                      ) -> bool:
            """
            Вызывает функцию шейпера входных данных из пайплайна
            и проверяет корректность возвращённых данных.

            :param self: Экземпляр задания.
            :param script_renderer: Рендерер скриптов.
            :param head_job_node: Имя ноды для головной задачи.
            :return: True, если данные валидны и скрипт создан.
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
            """
            Создаёт директории для обработки и хранения результатов.

            :param self: Экземпляр задания.
            :return: True, если директории созданы и доступны.
            """
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
            Формирует пути к служебным файлам Slurm без их создания.

            :param self: Экземпляр задания.
            :return: True, если пути сформированы успешно.
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
        self.created = self._update_time_mark()
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
        Переводит задание в состояние 'queued'.

        :param job_id: Идентификатор задачи в Slurm.
        """
        # Обновление меток времени
        now = self._update_time_mark()
        self.queued = now
        self.time_in_processing = "00:00:00"
        self.status = "queued"
        self.slurm_main_job = TaskSlurmJob(job_id)
        # Обновляем статус задания в SampleMeta
        self.sample_meta.tasks[self.pipeline.id] = {self.task_id:self.status}
        return None

    def _update(
                self,
                slurm_data: Dict[str, Any]
               ) -> None:
        """
        Обновляет состояние задания на основе данных из Slurm.

        :param slurm_data: Данные о задаче из squeue.
        """
        # Обновление меток времени
        now = self._update_time_mark()   
        self.time_in_processing = self._get_time_str((now - self.queued).total_seconds()) # type: ignore
        
        # Обновление данных заданий Slurm
        # обновляем данные главного задания
        if self.slurm_main_job:
            self.slurm_main_job._update(slurm_data)
            # Обновляем собственный статус
            self.status = 'processing' if self.slurm_main_job.status == 'RUNNING' else 'queued'
        # Обновляем данные дочерних задач
        child_jobs = slurm_data.get("child_jobs", {})
        if child_jobs:
            for job_id, job_data in child_jobs.items():
                if job_id in self.slurm_child_jobs:
                    self.slurm_child_jobs[job_id]._update(job_data)
                else:
                    self.slurm_child_jobs[job_id] = TaskSlurmJob.from_dict(job_data)
        # Обновляем статус задания в SampleMeta
        self.sample_meta.tasks[self.pipeline.id] = {self.task_id:self.status}
        return None

    def _complete(
                  self
                 ) -> None:
        """
        Завершает выполнение задания.

        Обновляет статус, собирает информацию о выходных файлах,
        обновляет метаданные образца и результатов.
        """
        # Обновление меток времени
        now = self._update_time_mark()
        self.finish = now
        if self.slurm_main_job:
            self.slurm_main_job._complete(
                                          now,
                                          self.data.head_job_exitcode_f
                                         )
            self.status = 'completed' if self.slurm_main_job.status == 'completed' else 'failed'
        
        # Поиск выходных файлов в папке результата
        self.data._check_output_files()
        # Дополнение выходных данных информацией из шейпера
        if self.pipeline.shape_output:
            shaper_data = self.pipeline.shape_output(
                                                     self.data.result_dir,
                                                     self.data.log_dir
                                                    )
            if shaper_data:
                for group, group_data in shaper_data.items():
                    if group in self.data.output_data:
                        self.data.output_data[group].update(group_data)
                    else:
                        self.data.output_data[group] = group_data

        # Добавляем информацию о выходных файлах в ResultMeta
        self.result_meta.results[self.pipeline.id] = self.data.output_data
        # Обновляем статус задания в SampleMeta
        self.sample_meta.tasks[self.pipeline.id] = {self.task_id:self.status}
        return None

    def _update_time_mark(
                           self
                          ) -> datetime:
        """
        Обновляет временную метку последнего изменения.

        :return: Текущее время в UTC.
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
        Преобразует количество секунд в строку формата ЧЧ:ММ:СС.

        :param seconds: Количество секунд.
        :return: Строка в формате 'HH:MM:SS'.
        """
        if all([
                seconds,
                seconds >= 1
               ]):
            hours, remainder = divmod(int(seconds), 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return "00:00:00"
