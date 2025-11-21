from tools.shaper_loader import load_shaper_functions
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path

@dataclass(slots=True)
class Pipeline:
    _cfg:Dict[str, Any]
    _shaper_dir:Path
    service_data: Dict[str, Any]
    id:str
    nextflow_config:Path
    name:str = field(default_factory=str)
    version:str = field(default_factory=str)
    # Функции для обработки данных
    shape_input:Optional[Callable] = field(default=None)
    shape_output:Optional[Callable] = field(default=None)
    conditions:List[Dict[str, Any]] = field(default_factory=list)
    sorting_type:str = field(default_factory=str)
    sorting_indicator:str = field(default_factory=str)
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
        self.name = self._cfg.get('name', "")
        self.version = self._cfg.get('version', "")
        self.shape_input, self.shape_output = load_shaper_functions(
                                                shaper_path=(self._shaper_dir / self._cfg.get('data_shaper', ""))
                                                                   )
        self.conditions = [
                           {
                            'field':condition.get('field'),
                            'type':condition.get('type'),
                            'value':condition.get('value')
                           } for condition in self._cfg.get('conditions', [])
                          ]
        sort_data = self._cfg.get('sorting')
        if sort_data:
            self.sorting_type, self.sorting_indicator = sort_data.split(',')
        self.timeout = self._cfg.get('timeout', "00:00")
        self.environment_variables = self._cfg.get('environment_variables', {})
        self.slurm_options = self._cfg.get('slurm_options', {})
        self.nextflow_variables = self._cfg.get('nextflow_variables', {})
        self.cmd_template = self._cfg.get('command_template', "")
        self.output_files_expected = self._cfg.get('output_files_expected', {})
        # Удаляем конфиг для экономии памяти
        self._cfg.clear()