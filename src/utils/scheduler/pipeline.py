from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

@dataclass(slots=True)
class Pipeline:
    _cfg:Dict[str, Any]
    name:str = field(default_factory=str)
    version:str = field(default_factory=str)
    id:str = field(default_factory=str)
    data_shaper:Path = field(default_factory=Path)
    conditions:List[Dict[str, Any]] = field(default_factory=list)
    slurm_resources:Dict[str, Any] = field(default_factory=dict)
    cmd_template:str = field(default_factory=str)
    output_files:List[str] = field(default_factory=list)
    compatible_samples:List[Dict[str, Any]] = field(default_factory=list)
    samples2process:set = field(default_factory=set)
    
    def __post_init__(self):
        self.name = self._cfg['name']
        self.version = self._cfg['version']
        self.id = f"{self.name}_{self.version}"
        self.data_shaper = self._cfg['input_data_shaper']
        self.conditions = [
                           {
                            'field':condition['field'],
                            'type':condition['type'],
                            'value':condition['value']
                           } for condition in self._cfg['conditions']
                          ]
        self.slurm_resources = self._cfg['resources']
        self.cmd_template = self._cfg['command_template']
        self.output_files = self._cfg['output_files']