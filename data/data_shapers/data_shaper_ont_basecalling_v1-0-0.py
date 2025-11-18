from src.utils.filesystem.metas import SampleMeta
from src.utils.scheduler.pipeline import Pipeline
from pathlib import Path
from typing import Any, Dict, Tuple

def generate_pipeline_input_data(
                                 sample_meta: SampleMeta,
                                 pipeline: Pipeline,
                                 service_data: Dict[str, Any]
                                ) -> Tuple[str, Path]:
    """
    Формирует данные для запуска пайплайна на основе метаданных образца, параметров пайплайна и сервисных данных.
    Возвращает подстроку команды nextflow и путь к tsv-файлу с данными.
    """
    