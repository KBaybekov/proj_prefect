import logging
from typing import Optional
from utils.common import env_var, log_error, run_shell_cmd
from utils.filesystem import is_module_installed

logger = logging.getLogger(__name__)  # наследует конфиг из watchdog.py


class SchedulerConnectionError(Exception):
    pass


def connect_scheduler() -> Optional[str]:
    """Проверка доступности БД
    ------------------
    :returns: None
    """
    list_of_schedulers = ['slurm']
    scheduler_type = env_var('SCHD_TYPE')
    # Проверка есть только для этих типов БД
    if scheduler_type not in list_of_schedulers:
        log_error(error_type=SchedulerConnectionError,
                  error_message="Неверно указан тип планировщика задач")
    
    # Ждём отклика по таймауту
    if scheduler_type == 'slurm':    
        if is_module_installed(module_name='pyslurm'):
            """
            Здесь должна выполняться проверка связи и работоспособности связки pyslurm,
            но на момент разработки модуль застрял на версии 24.11
            """
        else:
            slurmctld_active = run_shell_cmd(cmd='sinfo', timeout=1, debug='')['stdout']
            if slurmctld_active and 'up' in slurmctld_active:
                return scheduler_type
            else:
                log_error(error_type=SchedulerConnectionError,
                          error_message="Slurm не активен")
