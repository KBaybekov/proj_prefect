from jinja2 import Environment, FileSystemLoader, Template
from pathlib import Path
from typing import Dict, Optional
from utils.logger import get_logger

logger = get_logger(__name__)

class ScriptRenderer:
    """
    Класс для рендеринга bash-скриптов и строковых шаблонов с использованием Jinja2.

    Предназначен для генерации стартовых скриптов запуска задач в системе Slurm
    на основе шаблонов и переданных параметров. Также поддерживает рендеринг
    произвольных строковых шаблонов.
    """

    def __init__(
                 self,
                 starting_script_template: Path
                ):
        """
        Инициализирует рендерер шаблонов.

        Загружает шаблон стартового скрипта из указанного файла.
        При ошибке (файл не найден или ошибка загрузки) логирует проблему.

        :param starting_script_template: Путь к файлу шаблона стартового скрипта.
        :type starting_script_template: Path
        """
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
        Генерирует bash-скрипт запуска пайплайна на основе шаблона Jinja2.

        Подставляет переданные параметры в шаблон и сохраняет результат в файл.
        Создаёт директорию для логов, если она отсутствует.

        :param script_filename: Имя выходного файла скрипта.
        :type script_filename: str
        :param job_name: Имя задачи для Slurm.
        :type job_name: str
        :param work_dir: Рабочая директория выполнения.
        :type work_dir: Path
        :param res_dir: Директория для результатов.
        :type res_dir: Path
        :param log_dir: Директория для логов; в ней будет создан скрипт.
        :type log_dir: Path
        :param pipeline_timeout: Таймаут выполнения пайплайна в формате 'HH:MM' или 'D-HH:MM'.
        :type pipeline_timeout: str
        :param nxf_command: Команда запуска Nextflow.
        :type nxf_command: str
        :param head_job_node: Вычислительный узел, на котором запускается головная задача.
        :type head_job_node: str
        :param head_job_stdout: Путь к файлу stdout головной задачи.
        :type head_job_stdout: Path
        :param head_job_stderr: Путь к файлу stderr головной задачи.
        :type head_job_stderr: Path
        :param head_job_exitcode_f: Путь к файлу с кодом завершения головной задачи.
        :type head_job_exitcode_f: Path
        :param environment_variables: Переменные окружения для установки в скрипте.
        :type environment_variables: Optional[Dict[str, str]]
        :param nxf_variables: Переменные, передаваемые в Nextflow.
        :type nxf_variables: Optional[Dict[str, str]]
        :param slurm_options: Дополнительные опции для sbatch.
        :type slurm_options: Optional[Dict[str, str]]
        :return: Путь к сгенерированному скрипту или пустой Path при ошибке.
        :rtype: Path
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
        Рендерит произвольную строку-шаблон Jinja2 с переданными переменными.

        Если переменные переданы — выполняет подстановку.
        При ошибке рендеринга логирует её и возвращает исходную строку.
        Если переменные не переданы — возвращает строку без изменений.

        :param template: Строка с шаблоном Jinja2.
        :type template: str
        :param kwargs: Переменные для подстановки в шаблон.
        :return: Сгенерированная строка или исходная при ошибке.
        :rtype: str
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
