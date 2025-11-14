from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import subprocess


@dataclass(slots=True)
class SlurmJob:
    # Идентификаторы
    job_id:int = field(default_factory=int)
    parent_job_id:int = field(default_factory=int)
    name:str = field(default_factory=str)
    # Отслеживание
    work_dir:Path = field(default_factory=Path)
    partition:str = field(default_factory=str)
    priority:str = field(default="")
    nodes:str = field(default_factory=str)
    status:str = field(default="")
    stderr:Path = field(default_factory=Path)
    stdout:Path = field(default_factory=Path)
    exit_code:Optional[int] = field(default=None)
    # Время
    start:Optional[datetime] = field(default=None)
    finish:Optional[datetime] = field(default=None)
    limit:Optional[datetime] = field(default=None)

@dataclass(slots=True)
class TaskData:
    input_files:Dict[str,Set[Path]] = field(default_factory=dict)
    input_files_size:Dict[str,int] = field(default_factory=dict)
    input_data_shaper:Path = field(default_factory=Path)
    output_files_status:Dict[Path, bool] = field(default_factory=dict)
    output_files_expected:List[Path] = field(default_factory=list)
    output_data_shaper:Path = field(default_factory=Path)
    

@dataclass(slots=True)
class ProcTask:
    # Шаблон имени: sample_sample-fingerprint_pipeline-name_pipeline-version
    task_id:str
    sample:str = field(default_factory=str)
    pipeline_name:str = field(default_factory=str)
    sample_fingerprint:str = field(default_factory=str)
    pipeline_version:str = field(default_factory=str)
    # Файлы
    data: TaskData = field(default_factory=TaskData)
    # Папки
    result_dir:Path = field(default_factory=Path)
    # Время
    created:Optional[datetime] = field(default=None)
    finish:Optional[datetime] = field(default=None)
    last_update:Optional[datetime] = field(default=None)
    time_spent:Optional[datetime] = field(default=None)
    # Slurm
    status:str = field(default_factory=str)
    slurm_main_job:Optional[SlurmJob] = field(default=None)
    slurm_child_jobs:Dict[int, SlurmJob] = field(default_factory=dict)
    # Unix
    pid:int = field(default_factory=int)
    exit_code:Optional[int] = field(default=None)
    _subprocess:Optional[subprocess.Popen] = field(default=None)

    @staticmethod
    def from_dict(doc: Dict[str, Any]):
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
                               input_files_size=doc.get("input_files_size", {})
                              )
        return slurm_task
    
    def __post_init__(self):
        return

    def to_db(self) -> None:
        return