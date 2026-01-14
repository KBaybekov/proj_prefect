from tools.shaper_loader import load_shaper_functions
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path

@dataclass(slots=True)
class Pipeline:
    """
    Класс, представляющий конвейер обработки данных, описанный в конфигурационном файле.

    Содержит параметры выполнения, условия запуска, настройки окружения, пути к шейперам данных
    и другую метаинформацию, необходимую для создания и управления задачами обработки.
    """

    cfg:Dict[str, Any]
    """
    Исходный словарь конфигурации пайплайна.
    Используется для извлечения всех параметров при инициализации.
    """

    shaper_dir:Path
    """
    Базовая директория, в которой находятся модули формирования данных (шейперы).
    Используется для подгрузки функций shape_input и shape_output.
    """
    
    service_data: Dict[str, Any]
    """
    Сервисные файлы, используемые пайплайном (например, пути к референсам).
    Передаются в шейперы и могут использоваться при формировании команд.
    """
    
    id:str
    """
    Уникальный идентификатор пайплайна, составленный из имени и версии (например, 'ont-basecalling_v1-0-0').
    """

    nextflow_config:Path
    """
    Путь к конфигурационному файлу Nextflow, связанному с этим пайплайном.
    Может использоваться при запуске через Nextflow.
    """

    name:str = field(default_factory=str)
    """
    Название пайплайна
    """

    version:str = field(default_factory=str)
    """
    Версия пайплайна
    """

    # Функции для обработки данных
    shape_input:Optional[Callable] = field(default=None)
    """
    Функция для формирования входных данных задачи.
    Подгружается из файла, указанного в 'data_shaper' конфига.
    Принимает:
     - ProcessingTask
    Возвращает:
     - CLI команда Nextflow
     - все входные данные, распределённые по группам (есть отдельный ключ "size" с общим размером данных)
     - Группы ожидаемых выходных данных {группа_файлов:{тип_файлов:маска_файлов}}     
    """

    shape_output:Optional[Callable] = field(default=None)
    """
    Функция для формирования выходных данных задачи.
    Подгружается из файла, указанного в 'data_shaper' конфига.
    """

    conditions:List[Dict[str, Any]] = field(default_factory=list)
    """
    Список условий, которым должны удовлетворять образцы для запуска пайплайна.
    Каждое условие — словарь с полями: 'field', 'type', 'value'.
    Пример: [{'field': 'status', 'type': 'eq', 'value': 'indexed'}].
    """

    sorting:str = field(default_factory=str)
    """
    Тип сортировки при постановке в очередь: 'SJF' (по возрастанию), 'LJF' (по убыванию).
    Определяет порядок запуска задач при ограниченной ёмкости очереди.
    """

    timeout:str = field(default='00:00')
    """
    Максимальное время выполнения задачи в формате 'D-HH:MM' или 'HH:MM'.
    Передаётся в slurm-опции или используется для внутреннего контроля.
    Значение по умолчанию — '00:00'.
    """
    
    environment_variables:Dict[str, str] = field(default_factory=dict)
    """
    Переменные окружения, необходимые для выполнения пайплайна.
    Будут добавлены в стартовый скрипт.
    """

    nextflow_variables:Dict[str, str] = field(default_factory=dict)
    """
    Переменные Nextflow.
    Учитываются при запуске пайплайна Nextflow.
    """

    slurm_options:Dict[str, str] = field(default_factory=dict)
    """
    Настройки запуска в Slurm (например, --cpus-per-task, --mem).
    Передаются в sbatch при постановке задачи.
    """

    cmd_template:str = field(default_factory=str)
    """
    Шаблон команды запуска пайплайна.
    Может содержать плейсхолдеры, которые будут заменены при формировании финальной команды.
    """

    output_files_expected:Dict[str, str] = field(default_factory=dict)
    """
    Описание ожидаемых выходных файлов в формате {тип_файла: маска_поиска}.
    Используется для проверки результатов выполнения задачи.
    Пример: {'bam': '*.bam', 'bai': '*.bam.bai'}.
    """

    compatible_samples:List[Dict[str, Any]] = field(default_factory=list)
    """
    Список образцов, совместимых с данным пайплайном.
    Заполняется на этапе анализа совместимости.
    """
    
    def __post_init__(
                      self
                     ) -> None:
        """
        Метод, вызываемый после инициализации экземпляра класса.

        Заполняет атрибуты объекта на основе данных из cfg.
        Подгружает функции shape_input и shape_output из указанного модуля.
        """
        self.name = self.cfg.get('name', "")
        self.version = self.cfg.get('version', "")
        self.shape_input, self.shape_output = load_shaper_functions(
                                                shaper_path=(self.shaper_dir / self.cfg.get('data_shaper', ""))
                                                                   )
        self.conditions = [
                           {
                            'field':condition.get('field'),
                            'type':condition.get('type'),
                            'value':condition.get('value')
                           } for condition in self.cfg.get('conditions', [])
                          ]
        self.sorting = self.cfg.get('sorting', "")
        self.timeout = self.cfg.get('timeout', "00:00")
        self.environment_variables = self.cfg.get('environment_variables', {})
        self.slurm_options = self.cfg.get('slurm_options', {})
        self.nextflow_variables = self.cfg.get('nextflow_variables', {})
        self.cmd_template = self.cfg.get('command_template', "")
        self.output_files_expected = self.cfg.get('output_files_expected', {})

    @staticmethod
    def from_dict(
                  doc:Dict[str, Any]
                 ) -> 'Pipeline':
        """
        Создаёт экземпляр Pipeline из словаря (например, из документа MongoDB).

        :param doc: Словарь с данными пайплайна.
        :type doc: Dict[str, Any]
        :return: Новый экземпляр Pipeline.
        :rtype: Pipeline
        """
        pipeline = Pipeline(
                            cfg=doc.get('cfg', {}),
                            shaper_dir=doc.get('shaper_dir', Path()),
                            id=doc.get('id', ""),
                            nextflow_config=doc.get('nextflow_config', Path()),
                            service_data=doc.get('service_data', {})
                           )
        return pipeline
    
    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Преобразует экземпляр Pipeline в словарь для сохранения в БД.

        Сохраняет только ключевые поля: cfg, shaper_dir, id, nextflow_config, service_data.
        Пути преобразуются в строки в формате POSIX.

        :return: Сериализованный словарь с данными пайплайна.
        :rtype: Dict[str, Any]
        """
        keys2remove = []
        dict_obj = self.__dict__
        for key in dict_obj:

            if key not in [
                           "cfg",
                           "shaper_dir",
                           "id",
                           "nextflow_config",
                           "service_data"
                          ]:
                keys2remove.append(key)
            
            if key in ['shaper_dir', 'nextflow_config']:
                if dict_obj[key]:
                    dict_obj[key] = dict_obj[key].as_posix()
                else:
                    dict_obj[key] = ""

        for key in keys2remove:
            del dict_obj[key]
        return dict_obj
