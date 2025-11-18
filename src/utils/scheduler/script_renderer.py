from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path
from typing import Dict, Optional


class ScriptRenderer:
    def __init__(
                 self,
                 starting_script_template: Path
                ):
        self.env:Environment = Environment(
                                           loader=FileSystemLoader(starting_script_template.parent),
                                           autoescape=False,
                                           trim_blocks=True,
                                           lstrip_blocks=True
                                          )
        self.starting_script:Template = self.env.get_template(starting_script_template.name)

    def render_starting_script(
                               self,
                               script_filename: str,
                               job_name: str,
                               work_dir: Path,
                               res_dir: Path,
                               log_dir: Path,
                               pipeline_timeout: str,
                               nxf_command: str,
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
                                               HEAD_JOB_STDOUT=head_job_stdout,
                                               HEAD_JOB_STDERR=head_job_stderr,
                                               HEAD_JOB_EXITCODE_F=head_job_exitcode_f
                                              )
        # Создаём директорию, если её нет
        log_dir.mkdir(
                      parents=True,
                      exist_ok=True
                     )
        script_path = (log_dir / script_filename).resolve()
        script_path.write_text(rendered)
        return script_path

