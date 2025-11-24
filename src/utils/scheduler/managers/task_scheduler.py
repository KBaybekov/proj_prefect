from typing import Any, Dict, List, Optional, Set
from prefect import flow, task
from dataclasses import dataclass, field
from datetime import datetime, timezone
from utils.db.db import ConfigurableMongoDAO
from utils.logger import get_logger
from pathlib import Path
from .slurm_manager import SlurmManager
from scheduler import *
from classes.pipeline import Pipeline
from classes.processing_task import ProcessingTask
from tools.script_renderer import ScriptRenderer
import subprocess


logger = get_logger(__name__)


@dataclass(slots=True)
class TaskScheduler:
    _cfg:Dict[str, Any]
    dao:ConfigurableMongoDAO
    poll_interval:int = field(default=30)
    script_renderer:Optional[ScriptRenderer] = field(default=None)
    slurm_manager:Optional[SlurmManager] = field(default=None)
    pipelines: Dict[str, Pipeline] = field(default_factory=dict)
    head_job_node:str = field(default_factory=str)
    # словарь созданных задач {task_id: ProcessingTask}
    prepared_tasks:Dict[str, ProcessingTask] = field(default_factory=dict)
    # словарь задач, подготовка которых завершилась с ошибкой
    disprepared_tasks:Dict[str, ProcessingTask] = field(default_factory=dict)
    # словарь всех запущенных задач {task_id: ProcessingTask}
    queued_tasks: Dict[str, ProcessingTask] = field(default_factory=dict)
    # словарь запущенных задач, отсортированных по алгоритму Shortest Job First
    queued_tasks_sjf: Dict[str, ProcessingTask] = field(default_factory=dict)
    # словарь запущенных задач, отсортированных по алгоритму Longest Job First
    queued_tasks_ljf: Dict[str, ProcessingTask] = field(default_factory=dict)
    # словарь данных для идёмпотентной загрузки в БД вида {collection: {doc_id: doc_data}
    data_to_db:Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def init_scheduler(
                       self
                      ) -> None:
        # Инициализация загрузчика скриптов
        self.script_renderer = ScriptRenderer(starting_script_template=Path(self._cfg.get('start_script_template', '')))
        # Загрузка конфигурации пайплайнов
        self._load_pipelines()
        # Загрузка менеджера Slurm
        self._create_slurm_manager()
        # Актуализация информации о запущенных заданиях
        self._actualize_tasks_data()
        # Формирование заданий на обработку
        self._create_new_tasks()
        # Постановка заданий в очередь Slurm
        self._put_tasks_in_queue()
        # Удаляем конфиг для экономии памяти
        self._cfg.clear()
        return None
        
    def _load_pipelines(
                        self
                       ) -> None:
        """
        Загрузка конфигурации пайплайнов из общей конфигурации.
        """
        self.pipelines = {
                          pipeline_id:Pipeline(
                                               _cfg=pipeline_data,
                                               _shaper_dir=Path(self._cfg['shaper_dir']),
                                               id=pipeline_id,
                                               nextflow_config=self._cfg.get('nextflow_config', ''),
                                               service_data=self._cfg.get('service_data', {})
                                              )
                          for pipeline_id, pipeline_data in self._cfg['pipelines'].items()
                         }
        
        logger.info(f"Загружено {len(self.pipelines)} пайплайнов:\n{'\n\t'.join(self.pipelines.keys())}.")
        return None

    def _create_slurm_manager(
                              self
                             ) -> None:
        """
        Инициализация менеджера Slurm.
        """
        logger.debug("Инициализация менеджера Slurm...")
        self.slurm_manager = SlurmManager(
                                          _cfg=self._cfg.get('slurm', {})
                                         )
        logger.info("Менеджер Slurm инициализирован.")
        return None

    def _actualize_tasks_data(
                              self
                             ) -> None:
        """
        Выгружает из БД список созданных ранее задач, в т.ч. запущенных в обработку.
        Собирает данные о запущенных заданиях из Slurm.
        Актуализирует данные заданий.
        Загружает обновленные данные в БД.
        """
        # Обновляем информацию по созданным, но не запущенным заданиям
        logger.debug("Актуализация данных о созданных заданиях...")
        self.prepared_tasks.update(self._get_tasks_from_db(statuses=["prepared"]))
        # Обновляем информацию по запущенным заданиям
        logger.debug("Актуализация данных о запущенных заданиях...")
        unqueued_tasks:List[ProcessingTask] = []
        # Выгрузка из БД запущенных задач
        self.queued_tasks.update(self._get_tasks_from_db(statuses=["queued","running"]))
        # Обновление данных о запущенных заданиях с помощью Slurm
        if self.slurm_manager:
            self.slurm_manager._get_queued_tasks_data()
            for task_id, task in self.queued_tasks.items():
                if task.slurm_main_job:
                    slurm_data = self.slurm_manager.squeue_data.get(
                                        task.slurm_main_job.job_id,
                                        {})
                    # Если данные о задании получены, обновляем его
                    if slurm_data:
                        logger.debug(f"Обновление данных задания {task_id} из Slurm.")
                        task._update(slurm_data)
                    # Если нет, то выполняем процедуру завершения задания 
                    # и удаляем его из списка поставленных в очередь
                    else:
                        logger.debug(f"Данные задания {task_id} не получены из Slurm.")
                        logger.debug(f"Задание {task_id} помечено как завершённое.")
                        task._complete()
                        unqueued_tasks.append(task)
                self._add_task_to_db_uploading(task)

        # Удаление завершённых заданий из списка поставленных в очередь
        for task in unqueued_tasks:
            self._remove_task_from_queued_in_TaskScheduler(task)
        return None

    def _get_tasks_from_db(
                           self,
                           statuses: list
                          ) -> Dict[str, ProcessingTask]:
        """
        Выгружает из БД список задач, запущенных ранее в обработку.
        Возвращает словарь {job_id: ProcessingTask}, сохраняя его в self.queued_tasks.
        """
        dao_request = self.dao.find(
                                    collection="tasks",
                                    query={'status': {"$in":["queued","processing"]}},
                                    projection={}
                                   )
        if dao_request:
            logger.debug(f"Выгружено задач из БД: {len(dao_request)}")
        else:
            logger.debug("Выгружено поставленных в очередь задач из БД: 0")
        tasks = {
                 task_data['task_id']:ProcessingTask.from_dict(task_data)
                 for task_data in dao_request
                }
        return tasks

    def _add_task_to_db_uploading(
                                  self,
                                  task: ProcessingTask
                                 ) -> None:
        """
        Добавляет задание в список на выгрузку в БД.
        """
        collection = "tasks"
        self.data_to_db[collection][task.task_id] = task
        logger.debug(f"Задание {task.task_id} добавлено в список на выгрузку в БД.")
        return None

    def _remove_task_from_queued_in_TaskScheduler(
                                                  self,
                                                  task: ProcessingTask
                                                 ) -> None:
        if task.task_id in self.queued_tasks:
            try:
                del self.queued_tasks[task.task_id]
                logger.debug(f"Задание {task.task_id} удалено из общего списка поставленных в очередь.")
            except Exception as e:
                logger.error(f"Ошибка при удалении задания {task.task_id} из общего списка поставленных в очередь: {e}")
            # Удаляем из очередей по сортировке
            for sorting_type, queue in {
                                        "SJF": self.queued_tasks_sjf,
                                        "LJF": self.queued_tasks_ljf
                                       }.items():
                if task.sorting == sorting_type:
                    try:
                        del queue[task.task_id]
                        logger.debug(f"Задание {task.task_id} удалено из списка поставленных в очередь по алгоритму {sorting_type}.")
                    except Exception as e:
                        logger.error(f"Ошибка при удалении задания {task.task_id} из списка поставленных в очередь по алгоритму {sorting_type}: {e}")
        return None

    def _create_new_tasks(
                          self
                         ) -> None:
        """
        Создание новых задач на обработку из данных БД и данных класса.
        """
        # Формирование списка образцов, подходящих для обработки, для каждого пайплайна
        for pipeline_name, pipeline in self.pipelines.items():
            logger.debug(f"Пайплайн {pipeline_name}:")
            # запрашиваем данные образцов, подходящих для обработки, включая имя и список запущенных задач
            # включаем образцы, отвечающие заданным условиям
            # исключаем образцы, у которых в tasks уже есть этот пайплайн
            pipeline.compatible_samples = self.dao.find(
                                                        collection="samples",
                                                        query={'status':'indexed',
                                                               f'tasks.{pipeline.id}':{'$exists':False},
                                                               **{condition['field']: {f"${condition['type']}":condition['value']}
                                                                                       for condition in pipeline.conditions}
                                                              },
                                                        projection={"sample_id":1}                
                                                       )
            # Создаём задания на обработку
            if pipeline.compatible_samples:
                for sample in pipeline.compatible_samples:
                    self._create_task(
                                      sample['sample_id'],
                                      pipeline    
                                     )
        return None
               
    def _create_task(
                     self,
                     sample_id:str,
                     pipeline:Pipeline
                    ) -> Optional[ProcessingTask]:
        """
        Формирует задание на обработку
        """
        logger.debug(f"Создание задания обработки образца {sample_id} пайплайном {pipeline.id}")
        # Вытягиваем меты образца и его результатов. Создаём объект задания
        sample_meta:SampleMeta = self._get_obj_meta(sample_id, 'sample') # type: ignore
        result_meta:ResultMeta = self._get_obj_meta(sample_id, 'result') # type: ignore
        if all([sample_meta, result_meta]):
            task = ProcessingTask(
                                  sample_meta=sample_meta,
                                  result_meta=result_meta,
                                  pipeline=pipeline
                                 )
            logger.debug(f"Задание {task.task_id} инициализировано, подготовка данных...")
            # Генерируем данные для задания
            if self.script_renderer:
                task._prepare_data(
                                   self.script_renderer,
                                   self.head_job_node
                                  )
                if task.status == 'prepared':
                    logger.debug(f"Задание {task.task_id} подготовлено.")
                    # Добавляем задание в список созданных
                    self.prepared_tasks[task.task_id] = task
                else:
                    logger.error(f"Не удалось подготовить данные задания {task.task_id}: проблемы при создании")
                    self.disprepared_tasks[task.task_id] = task
            else:
                logger.error(f"Не удалось подготовить данные задания {task.task_id}: ScriptRenderer отсутствует")
        logger.error(f"Не удалось создать задание обработки образца {sample_id} пайплайном {pipeline.id}: недостаточно метаданных")
        return None        

    def _get_obj_meta(
                      self,
                      sample_id:str,
                      obj_type:str
                     ) -> Optional[ResultMeta|SampleMeta]:
        """
        Выгружает метаданные образца из БД. В случае отсутствия - логгирует ошибку.
        """
        meta = None
        logger.debug(f"Получение метаданных типа '{obj_type}' для образца {sample_id} из БД...")
        doc = self.dao.find_one(f'{obj_type}s', {'sample_id': sample_id})
        if doc:
            if obj_type == 'sample':
                meta = SampleMeta.from_db(doc)
            elif obj_type == 'result':
                meta = ResultMeta.from_db(doc)
            logger.debug(f"Метаданные '{obj_type}' для {sample_id} получены.")
        else:
            logger.error(f"Не удалось получить метаданные '{obj_type}' образца {sample_id} из БД.")
        return meta

    def _put_tasks_in_queue(
                            self
                           ) -> None:
        """
        Проверяет очереди Slurm на возможность запуска новых заданий.
        Сортирует ранее созданные задания в зависимости от:
        - типа сортировки, указанного в пайплайне (SJF, LJF);
        - максимальной длительности задания;
        - размера входных данных.
        Добавляет задания в очередь Slurm в порядке, определённом сортировкой.
        """
        def get_sort_key(task: ProcessingTask) -> float:
            def timeout_to_minutes(timeout: str) -> int:
                try:
                    if "-" in timeout:
                        days, hm = timeout.split("-")
                        days = int(days)
                    else:
                        days = 0
                        hm = timeout
                    hours, minutes = map(int, hm.split(":"))
                    return days * 1440 + hours * 60 + minutes
                except Exception as e:
                    logger.warning(f"Не удалось распарсить timeout '{timeout}': {e}")
                    return 0
            return task.data.input_files_size / timeout_to_minutes(task.pipeline.timeout)
        
        # Проверяем наличие подготовленных задач
        if not self.prepared_tasks:
            logger.debug("Нет задач со статусом 'prepared' для постановки в очередь.")
            return None

        if self.slurm_manager:
            available_slots = 0
            queues = {
                      "SJF": [self.queued_tasks_sjf, self.slurm_manager.sjf_queue_size],
                      "LJF": [self.queued_tasks_ljf, self.slurm_manager.ljf_queue_size]
                     }
            for queue_type, (queue, max_size) in queues.items():
                current_queue_usage = len(queue)
                available_slots = max(0, max_size - current_queue_usage)
                if available_slots == 0:
                    logger.debug(f"Достигнут лимит очереди {queue_type}. Новые задачи не ставятся.")
                    continue
                weighted_tasks = {
                                  task_id:get_sort_key(task) for task_id, task in self.prepared_tasks.items()
                                  if task.pipeline.sorting == queue_type
                                 }
                reverse_sort = queue_type == "LJF"
                sorted_tasks_ids = sorted(weighted_tasks.items(), key=lambda item:item[1], reverse=reverse_sort)
                # Ограничиваем по доступным слотам
                task_ids_to_submit = sorted_tasks_ids[:available_slots]
                for task_id, _ in task_ids_to_submit:
                    task = self.prepared_tasks.pop(task_id)
                    self.slurm_manager._submit_to_slurm(task)
                    queue[task_id] = task
                    self.queued_tasks[task_id] = task

        return None


   
    def _task_completion_check(
                               self,
                               task_record:SlurmTask
                              ) -> None:
         task_record.slurm_status == 'ok',
                task_record.nextflow_status == 'ok'
               ]):
            if all(val==True for val in task_record.output_files_status.values()):

    def _extract_task_results(task: SlurmTask) -> None:
    

    def _check_pipeline_requirements(self, sample: str, pipeline_name: str) -> bool:
        """Проверяет, удовлетворяет ли образец требованиям пайплайна."""
        pipeline = self.pipeline_config.get(pipeline_name)
        if not pipeline:
            logger.error(f"Пайплайн {pipeline_name} не найден в конфигурации.")
            return False

        sample_data = self.dao.find_one("samples", {"sample": sample})
        if not sample_data:
            logger.warning(f"Данные для образца {sample} не найдены.")
            return False

        for condition in pipeline.get("conditions", []):
            field = condition["field"]
            op = condition["type"]
            value = condition["value"]

            # Получение вложенного значения
            current_value = self._get_nested_value(sample_data, field)

            # Логика проверки
            if op == "eq":
                if current_value != value:
                    logger.debug(f"Условие {field} == {value} не выполнено.")
                    return False
            elif op == "ne":
                if isinstance(value, list):
                    if current_value in value:
                        logger.debug(f"Условие {field} ∉ {value} не выполнено.")
                        return False
                else:
                    if current_value == value:
                        logger.debug(f"Условие {field} != {value} не выполнено.")
                        return False
            elif op == "gte":
                if not isinstance(current_value, (int, float)) or current_value < value:
                    logger.debug(f"Условие {field} ≥ {value} не выполнено.")
                    return False
            # Добавьте остальные операторы аналогично
            elif op == "exists":
                if current_value is None:
                    logger.debug(f"Поле {field} отсутствует.")
                    return False
            # ...

        return True
    
    def _get_nested_value(self, data: dict, field: str):
        """Извлекает вложенное значение из словаря по пути, указанному в field."""
        if not isinstance(data, dict) or not field:
            return None

        parts = field.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None  # Путь недоступен
        return current

    def _generate_job_script(self, sample: str, pipeline_name: str, resources: Dict[str, Any]) -> str:
        """Генерирует bash-скрипт для Slurm."""
        script_path = Path("/tmp") / f"{pipeline_name}_{sample}.sh"
        with open(script_path, "w") as f:
            f.write("#!/bin/bash\n")
            for key, value in resources.items():
                f.write(f"#SBATCH --{key}={value}\n")
            f.write(f"cd {Path(__file__).parent.parent.parent}\n")
            f.write(f"./scripts/run_pipeline.sh {pipeline_name} {sample}\n")
        os.chmod(script_path, 0o755)
        return str(script_path)

    @task
    def start_scheduler(self) -> None:
        """Основной цикл планировщика задач."""
        while True:
            try:
                samples = self.dao.find("samples", {"status": "ready"})
                for sample in samples:
                    for pipeline_name in self.pipeline_config:
                        if self._check_pipeline_requirements(sample["sample"], pipeline_name):
                            self._submit_task(sample["sample"], pipeline_name)
                time.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"Ошибка в планировщике задач: {e}", exc_info=True)
                time.sleep(10)

    def _submit_task(self, sample: str, pipeline_name: str) -> None:
        """Формирует и отправляет задание в очередь Slurm."""
        pipeline = self.pipeline_config[pipeline_name]
        
        # Подготовка контекста (переменные для шаблона)
        context = {
            "input_dir": self._get_nested_value(sample_data, "input_dir"),
            "output_dir": self._get_nested_value(sample_data, "output_dir"),
            "threads": pipeline["resources"].get("cpus-per-task", 4),
            "sample": sample,
        }

        # Рендеринг команды через Jinja2
        template = Template(pipeline["command_template"])
        rendered_command = template.render(context)

        # Генерация bash-скрипта
        job_script = self._generate_job_script(rendered_command)
        
        # Отправка в Slurm
        job_id = self.slurm_manager.submit_to_slurm(job_script, f"{pipeline_name}_{sample}", pipeline["resources"])
        
        # Сохранение в БД
        task_collection.insert_one({
            "sample": sample,
            "pipeline": pipeline_name,
            "job_id": job_id,
            "status": "submitted",
            "created_at": datetime.now(timezone.utc),
            "output_files": pipeline["output_files"]
        })

    def check_output_files(self, task_doc: Dict[str, Any]) -> bool:
        """Проверяет наличие выходных файлов для задачи."""
        expected_files = task_doc.get("output_files", [])
        for file_path in expected_files:
            if not os.path.exists(file_path):
                return False
        return True

    def update_task_status(self, job_id: str, state: str, exit_code: Optional[str]) -> None:
        """Обновляет статус задачи в БД."""
        update_data = {"last_checked": datetime.now(timezone.utc)}
        
        if state == "COMPLETED":
            update_data["status"] = "ok"
        elif state == "FAILED":
            update_data["status"] = "fail"
            update_data["error_details"] = {"exit_code": exit_code}
        elif state == "RUNNING":
            update_data["status"] = "processing"
        else:
            update_data["status"] = "unknown"

        self.dao.update_one({"job_id": job_id}, {"$set": update_data})

    def retry_or_notify(self, task_doc: Dict[str, Any]) -> None:
        """Перезапускает задачу или отправляет уведомление об ошибке."""
        # Логика перезапуска или оповещения
        pass