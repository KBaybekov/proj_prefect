from typing import Any, Dict, List, Optional
from prefect import task
from threading import Timer
from dataclasses import dataclass, field
from utils.db.db import ConfigurableMongoDAO
from utils.logger import get_logger
from pathlib import Path
from .slurm_manager import SlurmManager
from scheduler import *
from classes.pipeline import Pipeline
from classes.processing_task import ProcessingTask
from tools.script_renderer import ScriptRenderer

logger = get_logger(__name__)

@dataclass(slots=True)
class TaskScheduler:
    """
    Класс-планировщик для управления жизненным циклом задач обработки образцов.

    Отвечает за:
    - Загрузку конфигураций пайплайнов
    - Формирование задач на обработку
    - Постановку задач в очередь Slurm
    - Актуализацию состояния запущенных задач
    - Выгрузку результатов в базу данных
    - Периодический мониторинг и планирование
    """

    _cfg:Dict[str, Any]
    """
    Конфигурационный словарь с настройками планировщика.
    Должен содержать: шаблоны скриптов, пути к шейперам, настройки Slurm, пайплайны и т.д.
    """
    
    dao:ConfigurableMongoDAO
    """
    Экземпляр DAO для взаимодействия с MongoDB.
    Используется для загрузки метаданных образцов, результатов и задач.
    """

    poll_interval:int = field(default=30)
    """
    Интервал (в секундах) между итерациями мониторинга.
    Определяет, как часто планировщик проверяет состояние задач и обновляет данные.
    """

    script_renderer:Optional[ScriptRenderer] = field(default=None)
    """
    Рендерер стартовых скриптов для задач.
    Инициализируется при запуске из шаблона, указанного в конфиге.
    """

    slurm_manager:Optional[SlurmManager] = field(default=None)
    """
    Менеджер взаимодействия с системой Slurm.
    Управляет постановкой задач в очередь, проверкой статусов и сбором данных.
    """

    pipelines: Dict[str, Pipeline] = field(default_factory=dict)
    """
    Словарь загруженных пайплайнов, индексированных по идентификатору.
    Заполняется при инициализации на основе конфигурации.
    """

    head_job_node:str = field(default_factory=str)
    """
    Имя вычислительного узла, на котором будет запущена головная задача (head job).
    Может использоваться для распределения нагрузки.
    """

    prepared_tasks:Dict[str, ProcessingTask] = field(default_factory=dict)
    """
    Словарь задач, подготовка которых завершена (статус 'prepared').
    Готовы к постановке в очередь Slurm.
    Ключ — task_id, значение — экземпляр ProcessingTask.
    """

    disprepared_tasks:Dict[str, ProcessingTask] = field(default_factory=dict)
    """
    Словарь задач, у которых возникли ошибки при подготовке данных.
    Хранится для диагностики и логирования.
    """

    queued_tasks: Dict[str, ProcessingTask] = field(default_factory=dict)
    """
    Общий словарь всех задач, поставленных в очередь Slurm (статус 'queued' или 'running').
    Используется для актуализации состояния.
    """

    queued_tasks_sjf: Dict[str, ProcessingTask] = field(default_factory=dict)
    """
    Словарь задач, поставленных в очередь с приоритетом SJF (Shortest Job First).
    Используется для контроля лимитов очереди.
    """

    queued_tasks_ljf: Dict[str, ProcessingTask] = field(default_factory=dict)
    """
    Словарь задач, поставленных в очередь с приоритетом LJF (Longest Job First).
    Используется для контроля лимитов очереди.
    """

    results2db:Dict[str, ResultMeta] = field(default_factory=dict)
    """
    Буфер для результатов, требующих выгрузки в БД.
    Ключ — sample_id, значение — экземпляр ResultMeta.
    Очищается после успешной выгрузки.
    """

    samples2db:Dict[str, SampleMeta] = field(default_factory=dict)
    """
    Буфер для метаданных образцов, требующих выгрузки в БД.
    Ключ — sample_id, значение — экземпляр SampleMeta.
    """

    tasks2db:Dict[str, ProcessingTask] = field(default_factory=dict)
    """
    Буфер для задач, требующих выгрузки в БД.
    Ключ — task_id, значение — экземпляр ProcessingTask.
    """

    db_timer:Optional[Timer] = field(default=None)
    """
    Таймер для периодического выполнения цикла мониторинга.
    Управляется методами start_scheduler и stop_scheduler.
    """
    
    def init_scheduler(
                       self
                      ) -> None:
        """
        Инициализирует компоненты планировщика перед запуском.

        - Создаёт script_renderer
        - Загружает пайплайны
        - Инициализирует slurm_manager
        - Выполняет начальную актуализацию задач и постановку в очередь
        - Очищает _cfg для экономии памяти
        """
        # Инициализация загрузчика скриптов
        self.script_renderer = ScriptRenderer(starting_script_template=Path(self._cfg.get('start_script_template', '')))
        # Загрузка конфигурации пайплайнов
        self._load_pipelines()
        # Загрузка менеджера Slurm
        self._create_slurm_manager()
        # Обновление данных планировщика, постановка новых заданий на обработку, выгрузка данных в БД
        self._update()
        # Удаляем конфиг для экономии памяти
        self._cfg.clear()
        return None

    def _update(
                self
               ) -> None:
        """
        Основной цикл обновления состояния планировщика.

        Выполняет:
        - Актуализацию данных о текущих задачах
        - Создание новых задач на основе доступных образцов
        - Постановку задач в очередь Slurm
        - Выгрузку накопленных изменений в БД
        """
        # Актуализация информации о запущенных заданиях
        self._actualize_tasks_data()
        # Формирование заданий на обработку
        self._create_new_tasks()
        # Постановка заданий в очередь Slurm
        self._put_tasks_in_queue()
        # Выгружаем данные в БД, если таковые имеются
        self._upload_data_to_db()
        return None

    def _load_pipelines(
                        self
                       ) -> None:
        """
        Загружает конфигурации пайплайнов из _cfg и создаёт экземпляры Pipeline.

        Для каждого пайплайна:
        - Создаёт объект Pipeline
        - Подгружает шейперы
        - Инициализирует условия, таймауты, переменные
        """
        self.pipelines = {
                          pipeline_id:Pipeline(
                                               cfg=pipeline_data,
                                               shaper_dir=Path(self._cfg['shaper_dir']),
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
        Создаёт и инициализирует экземпляр SlurmManager.

        Использует настройки из _cfg['slurm'].
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
        Актуализирует состояние всех задач, связанных с планировщиком.

        - Загружает из БД задачи в статусе 'prepared'
        - Загружает задачи в статусе 'queued' и 'running'
        - Обновляет их состояние с помощью Slurm
        - Помечает завершённые задачи
        - Удаляет завершённые задачи из очередей
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
                        if task.status == 'completed':
                             self.results2db.update({
                                                     task.sample_meta.sample_id:task.result_meta
                                                    })
                             self.samples2db.update({
                                                     task.sample_meta.sample_id:task.sample_meta
                                                    })
                        unqueued_tasks.append(task)
                self._add_task_to_db_uploading(task)

        # Удаление завершённых заданий из списка поставленных в очередь
        for task in unqueued_tasks:
            self._remove_task_from_queued_in_TaskScheduler(task)
        return None

    def _get_tasks_from_db(
                           self,
                           statuses:List[str]
                          ) -> Dict[str, ProcessingTask]:
        """
        Загружает из базы данных задачи с указанными статусами и восстанавливает связанные метаданные.

        Выполняет следующие действия:
        - Выполняет запрос к коллекции 'tasks' с фильтрацией по статусам
        - Для каждой задачи извлекает идентификатор образца (sample_id)
        - Подгружает соответствующие метаданные образца и результата, если они ещё не загружены
        - Создаёт экземпляры ProcessingTask с привязкой к метаданным

        Используется при актуализации состояния задач (например, при перезапуске планировщика
        или обновлении информации о запущенных/подготовленных заданиях).

        :param statuses: Список статусов задач, которые необходимо загрузить
                        (например, 'prepared', 'queued', 'running').
        :type statuses: List[str]
        :return: Словарь, где ключ — идентификатор задачи (task_id),
                значение — соответствующий экземпляр ProcessingTask.
        :rtype: Dict[str, ProcessingTask]
        """
        def add_metadata_to_obj_storage(
                                        obj_id:str,
                                        obj_storage:Dict[str, Any],
                                        obj_type:str,
                                       ) -> Dict[str, dict]:
            """
            Вспомогательная функция для загрузки метаданных образца или результата.

            Проверяет, загружены ли уже данные для указанного объекта.
            Если нет — запрашивает из БД и сохраняет в хранилище.

            :param obj_id: Идентификатор объекта (обычно sample_id).
            :type obj_id: str
            :param obj_storage: Словарь-хранилище для метаданных.
            :type obj_storage: Dict[str, Any]
            :param obj_type: Тип объекта: 'sample' или 'result'.
            :type obj_type: str
            :return: Обновлённое хранилище метаданных.
            :rtype: Dict[str, dict]
            """
            if obj_id not in obj_storage:
                collection = f'{obj_type}s'
                obj_storage[obj_id] = self.dao.find_one(
                                                        collection=collection,
                                                        query={'sample_id': obj_id},
                                                        projection={}
                                                       )
            return obj_storage

        samples = {}
        results = {}
        dao_request = self.dao.find(
                                    collection="tasks",
                                    query={'status': {"$in":statuses}},
                                    projection={}
                                   )
        if dao_request:
            logger.debug(f"Выгружено задач из БД: {len(dao_request)}")
        else:
            logger.debug("Выгружено поставленных в очередь задач из БД: 0")
        # Выгружаем также данные об образцах и результатах
        for task_data in dao_request:
            sample_id = task_data.get("sample_meta", "")
            if sample_id:
                for obj_type, obj_storage in {
                                             "sample": samples,
                                             "result": results
                                            }.items():
                    obj_storage = add_metadata_to_obj_storage(
                                                              obj_id=sample_id,
                                                              obj_storage=obj_storage,
                                                              obj_type=obj_type
                                                             )
        tasks = {
                 task_data['task_id']:ProcessingTask.from_dict(
                                                               doc=task_data,
                                                               result_meta=results.get(task_data.get("result_meta", ""), {}),
                                                               sample_meta=samples.get(task_data.get("sample_meta", ""), {})
                                                              )
                 for task_data in dao_request
                }
        return tasks

    def _add_task_to_db_uploading(
                                  self,
                                  task: ProcessingTask
                                 ) -> None:
        """
        Добавляет задачу в буфер для последующей выгрузки в БД.

        :param task: Задача для добавления.
        :type task: ProcessingTask
        """
        self.tasks2db[task.task_id] = task
        logger.debug(f"Задание {task.task_id} добавлено в список на выгрузку в БД.")
        return None

    def _remove_task_from_queued_in_TaskScheduler(
                                                  self,
                                                  task: ProcessingTask
                                                 ) -> None:
        """
        Удаляет задачу из всех очередей TaskScheduler.

        :param task: Задача для удаления.
        :type task: ProcessingTask
        """
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
        Создаёт новые задачи на обработку для совместимых образцов.

        Для каждого пайплайна:
        - Находит образцы, удовлетворяющие условиям
        - Исключает уже обработанные
        - Создаёт экземпляр ProcessingTask
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
                while pipeline.compatible_samples:
                    sample = pipeline.compatible_samples.pop(0)
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
        Создаёт и инициализирует задачу на обработку образца.

        :param sample_id: Идентификатор образца.
        :type sample_id: str
        :param pipeline: Пайплайн для обработки.
        :type pipeline: Pipeline
        :return: Созданная задача или None при ошибке.
        :rtype: Optional[ProcessingTask]
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
        Загружает метаданные образца или результата из БД.

        :param sample_id: Идентификатор образца.
        :type sample_id: str
        :param obj_type: Тип метаданных: 'sample' или 'result'.
        :type obj_type: str
        :return: Экземпляр метаданных или None.
        :rtype: Optional[ResultMeta | SampleMeta]
        """
        meta = None
        logger.debug(f"Получение метаданных типа '{obj_type}' для образца {sample_id} из БД...")
        doc = self.dao.find_one(f'{obj_type}s', {'sample_id': sample_id})
        if doc:
            if obj_type == 'sample':
                meta = SampleMeta.from_dict(doc)
            elif obj_type == 'result':
                meta = ResultMeta.from_dict(doc)
            logger.debug(f"Метаданные '{obj_type}' для {sample_id} получены.")
        else:
            logger.error(f"Не удалось получить метаданные '{obj_type}' образца {sample_id} из БД.")
        return meta

    def _put_tasks_in_queue(
                            self
                           ) -> None:
        """
        Поставляет подготовленные задачи в очередь Slurm.

        Задачи сортируются по типу (SJF/LJF) и приоритету.
        Учитывает лимиты очередей из slurm_manager.
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

    @task
    def start_scheduler(
                        self
                       ) -> None:
        """
        Запускает периодический мониторинг состояния задач.

        Инициализирует таймер, который каждые poll_interval секунд
        вызывает цикл обновления и актуализации.
        """
        # Переходим в режим мониторинга: периодически проверяем коллекции с данными на загрузку в БД на предмет... данных
        logger.info("Запуск мониторинга обработки данных...")
        self._monitor_scheduler()

    def _monitor_scheduler(
                           self
                          ) -> None:
        """
        Запускает периодический цикл мониторинга с использованием таймера.
        """
        def __check_new_data():
            self._update()
            self._monitor_scheduler()
        self.db_timer = Timer(self.poll_interval, __check_new_data)
        self.db_timer.start()

    def _upload_data_to_db(self) -> None:
        """
        Выгружает все накопленные изменения из буферов в MongoDB.
        Очищает буферы после успешной выгрузки.
        """
        def upload_data(
                        collection:str,
                        data:Dict[str, ProcessingTask|ResultMeta]
                       ) -> None:
            key_field = ""
            if data:
                if collection == "results":
                    key_field = "sample_id"
                elif collection == "tasks":
                    key_field = "task_id"
                for item_id, item in data.items():
                    if item:
                        self.dao.update_one(
                                            collection=collection,
                                            query={key_field: item_id},
                                            doc=item.to_dict()
                                           )
            return None
        
        data2upload:Dict[str, dict] = {
                                       "results":self.results2db,
                                       "samples":self.samples2db,
                                       "tasks":self.tasks2db
                                      }
        for collection, data_storage in data2upload.items(): 
            upload_data(collection, data_storage)
            data_storage.clear()

    def stop_scheduler(self) -> None:
        """
        Корректно останавливает планировщик.

        - Отменяет таймер мониторинга
        - Выполняет финальную выгрузку всех данных в БД
        - Логирует завершение
        """
        logger.info("Остановка TaskScheduler...")

        # 1. Останавливаем таймер мониторинга
        if self.db_timer is not None:
            logger.debug("Отмена таймера мониторинга планировщика...")
            self.db_timer.cancel()
            self.db_timer = None
            logger.debug("Таймер мониторинга остановлен")

        # 2. Выполняем финальную выгрузку накопленных данных в БД
        if any(self.tasks2db.values()) or any(self.results2db.values()) or any(self.samples2db.values()):
            logger.debug("Выполнение финальной выгрузки данных в БД...")
            self._upload_data_to_db()

        logger.info("TaskScheduler остановлен корректно")
