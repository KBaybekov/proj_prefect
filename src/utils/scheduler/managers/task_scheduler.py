from typing import Any, Dict, List, Optional, Set
from prefect import flow, task
from dataclasses import dataclass, field
from datetime import datetime, timezone
from utils.db.db import ConfigurableMongoDAO
from utils.logger import get_logger
from pathlib import Path
from .slurm_manager import SlurmManager
from utils.filesystem.metas import SampleMeta
from classes.pipeline import Pipeline
from classes.processing_task import ProcessingTask
import subprocess


logger = get_logger(__name__)


@dataclass(slots=True)
class TaskScheduler:
    _cfg:Dict[str, Any]
    dao:ConfigurableMongoDAO
    poll_interval:int = field(default=30)
    slurm_manager:Optional[SlurmManager] = field(default=None)
    pipelines: Dict[str, Pipeline] = field(default_factory=dict)
    created_tasks: Dict[str, ProcessingTask] = field(default_factory=dict)
    # словарь запущенных задач {task_id: ProcessingTask}
    queued_tasks: Dict[str, ProcessingTask] = field(default_factory=dict)
    # словарь данных для идёмпотентной загрузки в БД вида {collection: {doc_id: doc_data}
    data_to_db:Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def init_scheduler(
                       self
                      ) -> None:
        def __check_for_task_submission(pipeline_name: str, sample: str) -> dict:
            found_task = self.dao.find_one(
                                 collection="tasks",
                                 query={"pipeline": pipeline_name, "sample": sample}
                                )
            if found_task:
                logger.debug(f"Пайплайн {pipeline_name} для образца {sample} уже запущен.")
                return found_task
            logger.debug(f"Пайплайн {pipeline_name} для образца {sample} ранее запущен не был.")
            return {}
        
        # Загрузка конфигурации пайплайнов
        self._load_pipelines()
        # Загрузка менеджера Slurm
        self._create_slurm_manager()
        # Актуализация информации о запущенных заданиях
        self._actualize_queued_tasks_data()
        # Формирование заданий на обработку
        self._create_new_tasks()
        # Постановка заданий в очередь Slurm
        self._put_tasks_in_queue()
        # Удаляем конфиг для экономии памяти
        self._cfg.clear()


        return
        
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
                                               nextflow_config=self._cfg.get('nextflow_config', '')
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
                                          user=self._cfg['slurm_user'],
                                          poll_interval=int(self._cfg['slurm_poll_interval']),
                                          queue_size=int(self._cfg['slurm_queue_size'])
                                         )
        logger.info("Менеджер Slurm инициализирован.")
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
                                                        projection={
                                                                    "name":1,
                                                                    "fingerprint":1,
                                                                    "tasks":1
                                                                   }                
                                                       )
            # Если есть образцы, подходящие для обработки, проверяем, не было ли ранее создано задание на обработку
            # Иначе - создаём его
            if pipeline.compatible_samples:
                for sample in pipeline.compatible_samples:
                    # Формируем имя задачи
                    task_id = self._create_task_id(sample, pipeline)
                    # Проверяем на всякий случай, была ли задача запущена ранее, но не отмечена в sample.tasks
                    if self._task_exists(task_id):
                        self._add_task_to_sample_tasklist(sample, pipeline)
                        continue
                    else:
                        self._create_task(
                                          task_id,
                                          sample,
                                          pipeline    
                                         )
                        
    def _create_task(
                     self,
                     task_id:str,
                     sample:dict,
                     pipeline:Pipeline
                    ) -> Optional[ProcessingTask]:
        """
        Формирует задание на обработку
        """
        logger.debug(f"Создание задания {task_id}")
        # Запрашиваем данные образца из БД
        sample_db_data = self.dao.find_one(
                                           collection='samples',
                                           query={
                                                  'name': sample['name'],
                                                  'fingerprint': sample['fingerprint']
                                                 },
                                           projection={}
                                          )
        if sample_db_data:
            sample_meta = SampleMeta.from_db(sample_db_data)
            
        else:
            logger.error(f"Не удалось найти образец {sample['name']} с отпечатком {sample['fingerprint']} в БД.")

                        


    def _actualize_queued_tasks_data(
                                     self
                                    ) -> None:
        """
        Выгружает из БД список задач, запущенных ранее в обработку.
        Собирает данные о запущенных заданиях из Slurm.
        Актуализирует данные заданий.
        Загружает обновленные данные в БД.
        """
        unqueued_tasks:List[str] = []
        logger.debug("Актуализация данных о запущенных заданиях...")
        # Выгрузка из БД запущенных задач
        self._get_queued_tasks_from_db()
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
                        unqueued_tasks.append(task_id)
                self._add_task_to_db_uploading(task)

        # Удаление завершённых заданий из списка поставленных в очередь
        for task_id in unqueued_tasks:
            self._remove_task_from_queued_in_TaskScheduler(task_id)
        return None

    def _get_queued_tasks_from_db(
                                  self
                                 ) -> None:
        """
        Выгружает из БД список задач, запущенных ранее в обработку.
        Возвращает словарь {job_id: ProcessingTask}, сохраняя его в self.queued_tasks.
        """
        dao_request = self.dao.find(
                                    collection="tasks",
                                    query={'status': {"$in":["queued","running"]}},
                                    projection={}
                                   )
        if dao_request:
            logger.debug(f"Выгружено поставленных в очередь задач из БД: {len(dao_request)}")
        else:
            logger.debug("Выгружено поставленных в очередь задач из БД: 0")

        self.queued_tasks = {
                             task_data['task_id']:ProcessingTask.from_dict(task_data)
                             for task_data in dao_request
                            }
        return None

    def _add_task_to_db_uploading(
                                  self,
                                  task: ProcessingTask
                                 ) -> None:
        """
        Добавляет задание в список на выгрузку в БД.
        """
        collection = "tasks"
        self.data_to_db[collection][task.task_id] = task
        return None

    def _remove_task_from_queued_in_TaskScheduler(
                                                  self,
                                                  task_id: str
                                                 ) -> None:
        if task_id in self.queued_tasks:
            del self.queued_tasks[task_id]
            logger.debug(f"Задание {task_id} удалено из списка поставленных в очередь.")

    def _create_task_id(
                        self,
                        sample: Dict[str, Any],
                        pipeline:Pipeline
                       ) -> str:
        return f"{sample['sample']}_{sample['fingerprint']}_{pipeline.name}_{pipeline.version}"    
        

    def _task_exists(
                     self,
                     task_id: str
                    ) -> bool:
        return task_id in self.submitted_tasks


   
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