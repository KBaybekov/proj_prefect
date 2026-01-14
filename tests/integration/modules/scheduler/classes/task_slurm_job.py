from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from scheduler import *
from modules.logger import get_logger

logger = get_logger(__name__)

@dataclass(slots=True)
class TaskSlurmJob:
    """
    Класс для представления задачи в системе управления заданиями Slurm.

    Хранит информацию об идентификаторах, состоянии, ресурсах и временных метках задачи.
    Поддерживает создание из словаря, сериализацию в словарь и обновление данных из squeue.
    """
    # Идентификаторы
    job_id:int
    """
    Уникальный идентификатор задачи в системе Slurm. Обязательное поле.
    """

    parent_job_id:int = field(default=0)
    """
    Идентификатор родительской задачи, если текущая задача является частью массива или была запущена как зависимая.
    Значение 0 означает отсутствие родителя.
    """
    
    name:Optional[str] = field(default=None)
    """
    Имя задачи, заданное при её запуске (например, через #SBATCH --job-name).
    Может быть не указано.
    """
    
    # Отслеживание
    work_dir:Optional[Path] = field(default=None)
    """
    Рабочая директория задачи — путь, из которого была запущена задача.
    Используется для определения местоположения служебных файлов, таких как .exitcode.
    """

    partition:Optional[str] = field(default=None)
    """
    Раздел (партиция) Slurm, в котором выполняется задача (например, 'normal', 'long').
    Определяет доступные ресурсы и лимиты.
    """
    
    priority:Optional[str] = field(default=None)
    """
    Приоритет задачи в очереди Slurm. Может использоваться для внутреннего распределения ресурсов.
    """

    nodes:Optional[str] = field(default=None)
    """
    Имена выделенных вычислительных узлов, на которых выполняется задача.
    В случае нескольких узлов может содержать список или диапазон.
    """

    status:Optional[str] = field(default=None)
    """
    Текущий статус задачи в Slurm (например, PENDING, RUNNING, COMPLETED, FAILED, TIMEOUT).
    Обновляется при каждом опросе squeue.
    """

    stderr:Optional[Path] = field(default=None)
    """
    Путь к файлу, в который перенаправляется стандартный поток ошибок (STDERR) задачи.
    Определяется через #SBATCH --error.
    """

    stdout:Optional[Path] = field(default=None)
    """
    Путь к файлу, в который перенаправляется стандартный поток вывода (STDOUT) задачи.
    Определяется через #SBATCH --output.
    """

    exit_code:Optional[int] = field(default=None)
    """
    Код завершения выполнения задачи.
    0 — успешное завершение, другие значения — ошибка или таймаут.
    """

    # Время
    start:Optional[datetime] = field(default=None)
    """
    Время начала выполнения задачи в формате datetime.
    Устанавливается, когда задача переходит в статус RUNNING.
    """

    limit:Optional[datetime] = field(default=None)
    """
    Лимит времени выполнения задачи, заданный при запуске (например, через #SBATCH --time).
    Используется для определения максимального времени работы.
    """

    finish:Optional[datetime] = field(default=None)
    """
    Время завершения задачи.
    Устанавливается при завершении выполнения задачи.
    """

    @staticmethod
    def from_dict(squeue_data:Dict[str, Any]) -> 'TaskSlurmJob':
        """
        Создаёт экземпляр TaskSlurmJob из словаря данных, полученных от squeue.

        Используется для инициализации объекта на основе вывода команды squeue --json.

        :param squeue_data: Словарь с данными о задаче из squeue.
        :type squeue_data: Dict[str, Any]
        :return: Новый экземпляр TaskSlurmJob.
        :rtype: TaskSlurmJob
        """
        return TaskSlurmJob(
                            job_id=squeue_data.get('job_id', 0),
                            parent_job_id=squeue_data.get('parent_job_id', 0),
                            name=squeue_data.get('name'),
                            work_dir=squeue_data.get('work_dir'),
                            partition=squeue_data.get('partition'),
                            priority=squeue_data.get('priority'),
                            nodes=squeue_data.get('nodes'),
                            status=squeue_data.get('status'),
                            stderr=squeue_data.get('stderr'),
                            stdout=squeue_data.get('stdout'),
                            exit_code=squeue_data.get('exit_code'),
                            start=squeue_data.get('start'),
                            limit=squeue_data.get('limit')
                           )
    
    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Преобразует экземпляр TaskSlurmJob в словарь для последующей сериализации.

        Пути (work_dir, stderr, stdout) преобразуются в строки с использованием .as_posix().

        :return: Словарь с атрибутами объекта.
        :rtype: Dict[str, Any]
        """
        dict_obj = self.__dict__
        for key in dict_obj:

            if key in ['work_dir', 'stderr', 'stdout']:
                dict_obj[key] = dict_obj[key].as_posix()
        return dict_obj

    def _update(self, slurm_data:Dict[str, Any]):
        """
        Обновляет поля задачи на основе новых данных из Slurm.

        Исключает обновление job_id, start и limit.
        Если задача находится в статусе RUNNING, обновляет временные метки start и limit,
        если они ещё не установлены.

        :param slurm_data: Словарь с новыми данными от Slurm.
        :type slurm_data: Dict[str, Any]
        """
        if slurm_data:
            for attr, value in self.__dict__.items():
                if attr in ['job_id', 'start', 'limit']: continue
                setattr(self, attr, slurm_data.get(attr, value))
            self.status = slurm_data['status']

            # Если задача запущена, собираем данные по лимитам времени
            if self.status == 'RUNNING':
                for property in ['start', 'limit']:
                    if all([getattr(self, property) is None,
                            property in slurm_data]):
                        setattr(
                                self,
                                property,
                                slurm_data[property]
                               )
        
        else:
            self._complete()

    def _complete(
                  self,
                  now:Optional[datetime] = None,
                  exit_code_f:Optional[Path] = None
                 ) -> None:
        """
        Отмечает завершение задачи.

        Устанавливает время завершения и собирает данные о результатах выполнения.
        Если время не передано, используется текущее время в UTC.

        :param now: Время завершения задачи. По умолчанию — текущее время.
        :type now: Optional[datetime]
        :param exit_code_f: Путь к файлу с кодом завершения. Если не указан, используется work_dir/.exitcode.
        :type exit_code_f: Optional[Path]
        """
        if not now:
            now = datetime.now(timezone.utc)
        self.finish = now
        self._collect_completed_process_data(exit_code_f)
        return None

    def _collect_completed_process_data(
                                        self,
                                        exit_code_f:Optional[Path] = None
                                       ) -> None:
        """
        Собирает данные о завершённом процессе, читая код завершения из файла.

        Файл .exitcode ожидается в рабочей директории задачи, если путь не указан явно.
        Если файл не найден или произошла ошибка чтения, статус задачи определяется как FAILED.

        :param exit_code_f: Путь к файлу .exitcode. Если None, используется work_dir/.exitcode.
        :type exit_code_f: Optional[Path]
        """
        if self.work_dir:
            if not exit_code_f:
                exit_code_f = (self.work_dir / '.exitcode').resolve()
            logger.debug(f"Проверяем {exit_code_f}")
            if exit_code_f.exists():
                logger.debug(f"Найден .exitcode в {self.work_dir.as_posix()}:")
                try:
                    # Читаем первую строку и преобразуем в число
                    with open(exit_code_f, 'r') as f:
                        self.exit_code = int(f.readline().strip())
                        logger.debug(f"exit_code: {self.exit_code}")
                    return None
                except Exception as e:
                    logger.error(f"Ошибка при чтении {exit_code_f.as_posix()}: {e}")
            else:
                logger.error(f"Не найден .exitcode в {self.work_dir.as_posix()}")
            
        self._define_task_status_by_exit_code()
        return None

    def _define_task_status_by_exit_code(
                                         self
                                        ) -> None:
        """
        Определяет конечный статус задачи на основе её кода завершения.

        Статусы:
        - 0 → "COMPLETED"
        - 124 → "TIMEOUT"
        - Другие целые значения → "FAILED"
        - None → "UNKNOWN"
        """
        statuses = {
                    0: "COMPLETED",
                    124: "TIMEOUT"
                   }
        if self.exit_code is None:
            self.status = "UNKNOWN"
        elif isinstance(self.exit_code, int):
            self.status = statuses.get(self.exit_code, "FAILED")
