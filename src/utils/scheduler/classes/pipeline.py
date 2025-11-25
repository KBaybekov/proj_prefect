from tools.shaper_loader import load_shaper_functions
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path

@dataclass(slots=True)
class Pipeline:
    cfg:Dict[str, Any]
    shaper_dir:Path
    service_data: Dict[str, Any]
    id:str
    nextflow_config:Path
    name:str = field(default_factory=str)
    version:str = field(default_factory=str)
    # Функции для обработки данных
    shape_input:Optional[Callable] = field(default=None)
    shape_output:Optional[Callable] = field(default=None)
    conditions:List[Dict[str, Any]] = field(default_factory=list)
    sorting:str = field(default_factory=str)
    timeout:str = field(default_factory=str)
    environment_variables:Dict[str, str] = field(default_factory=dict)
    nextflow_variables:Dict[str, str] = field(default_factory=dict)
    slurm_options:Dict[str, str] = field(default_factory=dict)
    cmd_template:str = field(default_factory=str)
    # словарь вида {тип_файлов: маска}
    output_files_expected:Dict[str, str] = field(default_factory=dict)
    compatible_samples:List[Dict[str, Any]] = field(default_factory=list)
    samples2process:set = field(default_factory=set)
    
    def __post_init__(self):
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
    def from_dict(doc:Dict[str, Any]) -> 'Pipeline':
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
        Конвертирует объект SourceFileMeta в словарь.
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
