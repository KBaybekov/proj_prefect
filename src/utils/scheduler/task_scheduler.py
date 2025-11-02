from typing import Any, Dict, List, Optional, Set
from prefect import flow, task
from dataclasses import dataclass, field
from datetime import datetime, timezone
from utils.db.db import ConfigurableMongoDAO
from utils.logger import get_logger
from pathlib import Path
from .slurm_manager import SlurmManager
from utils.filesystem.metas import SampleMeta

logger = get_logger(__name__)


class Pipeline:
    def __init__(
                 self,
                 config: Dict[str, Any]
                ):
        self.name:str = config['name']
        self.version:str = config['version']
        self.id:str = f"{self.name}_{self.version}"
        self.data_shaper:Path = config['input_data_shaper']
        self.conditions:List[Dict[str, Any]] = [
                                                {
                                                 'field':condition['field'],
                                                 'type':condition['type'],
                                                 'value':condition['value']
                                                } for condition in config['conditions']
                                               ]
        self.slurm_resources:Dict[str, Any] = config['resources']
        self.cmd_template:str = config['command_template']
        self.output_files:List[str] = config['output_files']
        self.compatible_samples:List[Dict[str, Any]] = []
        self.samples2process:set = set()

@dataclass(slots=True)
class SlurmTask:
    # Шаблон имени: sample_sample_fingerprint_pipeline_name_pipeline_version
    task_id:str 
    sample:str = field(default_factory=str)
    pipeline_name:str = field(default_factory=str)
    sample_fingerprint:str = field(default_factory=str)
    pipeline_version:str = field(default_factory=str)
    slurm_job_id:str = field(default_factory=str)
    slurm_status:str = field(default="")
    nextflow_status:str = field(default="")
    output_files_status:Dict[Path, bool] = field(default_factory=dict)
    work_dir:Path = field(default_factory=Path)
    result_dir:Path = field(default_factory=Path)
    estimated_completion_time:Optional[datetime] = field(default=None)
    start:Optional[datetime] = field(default=None)
    finish:Optional[datetime] = field(default=None)
    time_spent:Optional[datetime] = field(default=None)
    output_files_expected:List[Path] = field(default_factory=list)
    # Словарь вида {тип файлов:список файлов}
    input_files:Dict[str,Set[Path]] = field(default_factory=dict)
    input_files_size:Dict[str,int] = field(default_factory=dict)
    error_data:Dict[str, Any] = field(default_factory=dict)
    slurm_queue:str = field(default_factory=str)
    slurm_queue_position:int = field(default_factory=int)
    
    @staticmethod
    def from_db(doc: Dict[str, Any]) -> SlurmTask:
        """
        Создаёт объект Task из документа БД.

        :param doc: Документ из коллекции 'tasks' в MongoDB.
        :return: Объект Task.
        """
        # Инициализируем основные поля SampleMeta
        slurm_task = SlurmTask(
                               task_id=doc.get("task_id", ""),
                               sample=doc.get("sample", ""),
                               pipeline_name=doc.get("pipeline_name", ""),
                               sample_fingerprint=doc.get("sample_fingerprint", ""),
                               pipeline_version=doc.get("pipeline_version", ""),
                               slurm_job_id=doc.get("slurm_job_id", ""),
                               slurm_status=doc.get("slurm_status", ""),
                               nextflow_status=doc.get("nextflow_status", ""),
                               output_files_status=doc.get("output_files_status", {}),
                               work_dir=Path(doc.get("work_dir", "")),
                               result_dir=Path(doc.get("result_dir", "")),
                               estimated_completion_time=doc.get("estimated_completion_time"),
                               start=doc.get("start"),
                               finish=doc.get("finish"),
                               time_spent=doc.get("time_spent"),
                               output_files_expected=doc.get("output_files_expected", []),
                               input_files=doc.get("input_files", {}),
                               input_files_size=doc.get("input_files_size", {}),
                               error_data=doc.get("error_data", {}),
                              )
        return slurm_task
    
    def to_db(self) -> None:
        return


class TaskScheduler:
    def __init__(
                 self,
                 dao: ConfigurableMongoDAO,
                 poll_interval: int = 60
                ):
        self.dao:ConfigurableMongoDAO = dao
        self.poll_interval:int = poll_interval
        self.submitted_tasks:Dict[str, SlurmTask] = {
                                                     task_from_db['name']: SlurmTask.from_db(task_from_db)
                                                     for task_from_db in self.dao.find(
                                                                                       collection="tasks",
                                                                                       query={'status': {"ne":"finished"}},
                                                                                       projection={}
                                                                                      )
                                                    }


    def init_scheduler(
                       self,
                       pipeline_cfg:Dict[str, Dict[str, Any]]
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
        self.pipelines: Dict[str, Pipeline] = {
                                               pipeline:Pipeline(pipeline_data)
                                               for pipeline, pipeline_data in pipeline_cfg.items()
                                              }
        logger.info(f"Загружено {len(self.pipelines)} пайплайнов:\n{'\n\t'.join(self.pipelines.keys())}.")
        # Загрузка менеджера Slurm
        self.slurm_manager = SlurmManager()
        logger.info("Менеджер Slurm инициализирован.")
        '''
        # Формирование массива образцов, готовых к обработке
        process_ready_samples = self.dao.find(
                                              collection="samples",
                                              query={'status': 'indexed'},
                                              projection={}
                                              )
        logger.debug(f"Образцов готово к обработке: {len(process_ready_samples)}.")
        '''
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
                    if self._is_submitted_task_in_db(task_id):
                        # Обновляем информацию о задаче в samples & tasks
                        self._update_task_data(task_id)
                    else:
                        pipeline.samples2process.add(sample)
            # !!! Дальше нам надо будет создать задания для подходящих образцов (как насчёт формирования очередей по возрастанию длительности обработки?)

            return
        
    def _create_task_id(
                        self,
                        sample: Dict[str, Any],
                        pipeline:Pipeline
                       ) -> str:
        return f"{sample['sample']}_{sample['fingerprint']}_{pipeline.name}_{pipeline.version}"    
        
    def _is_submitted_task_in_db(
                                 self,
                                 task_id: str
                                ) -> bool:
        if self.dao.find_one(
                             collection="tasks",
                             query={'task_id': task_id},
                             projection={'_id':1}
                            ):
            return True
        return False

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