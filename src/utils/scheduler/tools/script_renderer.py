from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path
from typing import Dict, Optional
from utils.logger import get_logger

logger = get_logger(__name__)

class ScriptRenderer:
    def __init__(
                 self,
                 starting_script_template: Path
                ):
        try:
            self.env:Environment = Environment(
                                            loader=FileSystemLoader(starting_script_template.parent),
                                            autoescape=False,
                                            trim_blocks=True,
                                            lstrip_blocks=True
                                            )
            self.starting_script:Template = self.env.get_template(starting_script_template.name)
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                logger.error(f"Шаблон запуска задачи не найден: {starting_script_template}")
            else:
                logger.error(f"Ошибка при создании класса ScriptRenderer: {e}")

    def render_starting_script(
                               self,
                               script_filename: str,
                               job_name: str,
                               work_dir: Path,
                               res_dir: Path,
                               log_dir: Path,
                               pipeline_timeout: str,
                               nxf_command: str,
                               head_job_node: str,
                               head_job_stdout:Path,
                               head_job_stderr:Path,
                               head_job_exitcode_f:Path,
                               environment_variables: Optional[Dict[str, str]],
                               nxf_variables: Optional[Dict[str, str]],
                               slurm_options: Optional[Dict[str, str]]                             
                              ) -> Path:
        """
        Генерирует bash-файл запуска пайплайна из Jinja2-шаблона.
        Возвращает путь к сгенерированному файлу.
        """
        script_path = Path()
        try:
            # Создание шаблона
            rendered = self.starting_script.render(
                                                   JOB_NAME=job_name,
                                                   WORK_DIR=work_dir,
                                                   RES_DIR=res_dir,
                                                   LOG_DIR=log_dir,
                                                   PIPELINE_TIMEOUT=pipeline_timeout,
                                                   NXF_COMMAND=nxf_command,
                                                   ENV_VARS=environment_variables,
                                                   NXF_VARIABLES=nxf_variables,
                                                   SLURM_OPTIONS=slurm_options,
                                                   HEAD_JOB_NODE=head_job_node,
                                                   HEAD_JOB_STDOUT=head_job_stdout,
                                                   HEAD_JOB_STDERR=head_job_stderr,
                                                   HEAD_JOB_EXITCODE_F=head_job_exitcode_f
                                                  )
        except Exception as e:
            if isinstance(e, KeyError):
                logger.error(f"Ошибка при рендеринге шаблона запуска задачи {script_filename}: не найдены необходимые переменные")
            else:
                logger.error(f"Ошибка при рендеринге шаблона запуска задачи {script_filename}: {e}")
            return Path()
        if rendered:
            try:
                # Создаём директорию, если её нет
                log_dir.mkdir(
                            parents=True,
                            exist_ok=True
                            )
                script_path = (log_dir / script_filename).resolve()
                if rendered:
                    script_path.write_text(rendered)
            except Exception as e:
                logger.error(f"Ошибка при записи шаблона запуска задачи {script_filename}: {e}")
                return Path()
        else:
            logger.error(f"Ошибка при рендеринге шаблона запуска задачи {script_filename}: пустая строка")
            return Path()
        return script_path

    def render_string(
                      self,
                      template: str,
                      **kwargs
                     ) -> str:
        """
        Принимает шаблон строки и произвольные аргументы.
        Возвращает сгенерированную строку.
        """
        if kwargs:
            try:
                # Создаём временный шаблон из строки
                jinja_template = self.env.from_string(template)
                return jinja_template.render(**kwargs)
            except Exception as e:
                logger.error(f"Ошибка при рендеринге строки: {e}")
        else:
            logger.error("Ошибка при рендеринге строки: не переданы аргументы")        
        # Если нет переменных — возвращаем как есть
        return template
