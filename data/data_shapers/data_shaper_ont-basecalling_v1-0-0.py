# -*- coding: utf-8 -*-
"""
Формирование входных и выходных данных для пайплайна
Пайплайн: ont-basecalling_v1.0.0
"""
from __future__ import annotations
from . import *
from utils.logger import get_logger

logger = get_logger(name=__name__)

def get_nxf_cfg_arg(
                    cfg:Optional[str]
                   ) -> str:
    if cfg:
        return f"-c {Path(cfg).resolve().as_posix()}"
    return ""

def generate_pipeline_input_data(
                                 cmd_template: str,
                                 task:ProcessingTask,
                                 service_data: Dict[str, Any]
                                ) -> Tuple[str, Path]:
    """
    Формирует данные для запуска пайплайна на основе метаданных образца, параметров пайплайна и сервисных данных.
    Возвращает подстроку команды nextflow и путь к tsv-файлу с данными.
    """
    arg_nxf_cfg = get_nxf_cfg_arg(service_data.get('nextflow_config'))
    arg_job_name = task.task_id
    arg_sample = task.sample_id






def generate_pipeline_output_data(
                                  result_dir: Path,
                                  qc_dir: Path,
                                  log_dir: Path
                                 ) -> Dict[str, Dict[str, Any]]:
    """
    Формирует данные для загрузки в БД.
    Возвращает словарь вида {коллекция: {вид данных: данные}}
    """
