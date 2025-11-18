from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

@dataclass(slots=True)
class Pipeline:
    _cfg:Dict[str, Any]
    id:str
    name:str = field(default_factory=str)
    version:str = field(default_factory=str)
    input_data_shaper:Path = field(default_factory=Path)
    output_data_shaper:Path = field(default_factory=Path)
    conditions:List[Dict[str, Any]] = field(default_factory=list)
    sorting_type:str = field(default_factory=str)
    sorting_indicator:str = field(default_factory=str)
    timeout:str = field(default_factory=str)
    sbatch_params:Dict[str, str] = field(default_factory=dict)
    cmd_template:str = field(default_factory=str)
    # словарь вида {тип_файлов: маска}
    output_files_expected:Dict[str, str] = field(default_factory=dict)
    compatible_samples:List[Dict[str, Any]] = field(default_factory=list)
    samples2process:set = field(default_factory=set)
    
    def __post_init__(self):
        self.name = self._cfg.get('name', "")
        self.version = self._cfg.get('version', "")
        self.input_data_shaper = Path(self._cfg.get('input_data_shaper', ""))
        self.output_data_shaper = Path(self._cfg.get('output_data_shaper', ""))
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
        self.sbatch_params = self._cfg.get('sbatch_params', {})
        self.cmd_template = self._cfg.get('command_template', "")
        self.output_files_expected = self._cfg.get('output_files_expected', {})
        # Удаляем конфиг для экономии памяти
        self._cfg.clear()