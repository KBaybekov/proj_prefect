from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Set
from scheduler import *
from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass(slots=True)
class TaskData:
    """
    Класс для хранения данных, связанных с выполнением задачи обработки.

    Содержит пути к директориям и файлам, информацию о входных и выходных данных,
    а также служебные файлы для интеграции с системой управления заданиями Slurm.
    """

    input_data:Dict[str, Set[Path]] = field(default_factory=dict)
    """
    Входные данные для пайплайна.
    Формат: {группа_файлов: множество_путей_к_файлам}.
    Пример: {'fast5': {Path('/data/fast5/file1.fast5'), ...}, 'fastq.gz': {...}}
    """
    
    input_files_size:int = field(default=0)
    """Общий размер входных файлов в байтах."""
    
    expected_output_data:Dict[str, Dict[str,str]] = field(default_factory=dict)
    """
    Описание ожидаемых выходных данных.
    Формат: {группа_результатов: {тип_файла: маска_поиска}}.
    Пример: {'QC': {'fastqc': '*fastqc.html', 'multiqc': 'multiqc_report.html'}}
    Используется для поиска и валидации результатов обработки.
    """
    
    output_data:Dict[str, Dict[str, Any]] = field(default_factory=dict)
    """
    Выходные данные после выполнения задания.
    Формат: {группа_результатов: {тип_данных: данные}}.
    Заполняется методом _check_output_files() и шейпером пайплайна.
    """
    
    output_files_size:int = field(default=0)
    """Общий размер выходных файлов в байтах."""
    
    start_script:Path = field(default_factory=Path)
    """Путь к сгенерированному стартовому скрипту для запуска пайплайна."""
    
    work_dir:Path = field(default_factory=Path)
    """Рабочая директория для временных файлов пайплайна."""
    
    result_dir:Path = field(default_factory=Path)
    """Директория для хранения результатов обработки."""
    
    log_dir:Path = field(default_factory=Path)
    """Директория для хранения логов выполнения."""
    
    head_job_stdout:Path = field(default_factory=Path)
    """Путь к файлу stdout головной задачи Slurm."""
    
    head_job_stderr:Path = field(default_factory=Path)
    """Путь к файлу stderr головной задачи Slurm."""
    
    head_job_exitcode_f:Path = field(default_factory=Path)
    """Путь к файлу с кодом завершения головной задачи Slurm."""

    @staticmethod
    def from_dict(doc: Dict[str, Any]) -> 'TaskData':
        """
        Создаёт объект TaskData из словаря (документа MongoDB).

        Выполняет преобразование строковых путей в объекты Path
        и восстанавливает структуру вложенных словарей с множествами.

        :param doc: Словарь с данными задачи.
        :return: Экземпляр TaskData.
        """
        def __extract_input_data_from_doc(
                                          item: Dict[str, Any]
                                         ) -> Dict[str, Set[Path]]:
            """
            Преобразует словарь входных данных из строки в Path.

            :param item: Словарь вида {группа: [список_путей_как_строки]}.
            :return: Словарь вида {группа: {множество_объектов_Path}}.
            """
            extracted_data = {}
            for group_name, files in item.items():
                extracted_data[group_name] = set([Path(f) for f in files])
            return extracted_data

        def __extract_output_data_from_doc(
                                           item: Dict[str, Any]
                                          ) -> Dict[str, Dict[str,Set[Path]]]:
            """
            Преобразует словарь выходных данных из строки в Path.

            :param item: Словарь вида {группа: {тип: [список_путей_как_строки]}}.
            :return: Словарь вида {группа: {тип: {множество_объектов_Path}}}.
            """
            extracted_data = {}
            for group_name, group_data in item.items():
                unpacked_data = {}
                for filetype, files in group_data.items():
                    unpacked_data[filetype] = set([Path(f) for f in files])
                extracted_data[group_name] = unpacked_data
            return extracted_data

        task_data = TaskData(
                             input_data=__extract_input_data_from_doc(doc.get("input_data", {})),
                             expected_output_data=doc.get("expected_output_data", {}),
                             output_data=__extract_output_data_from_doc(doc.get("output_data", {})),
                             input_files_size=doc.get("input_files_size", 0),
                             output_files_size=doc.get("output_files_size", 0),
                             start_script=Path(doc.get("start_script", "")),
                             work_dir=Path(doc.get("work_dir", "")),
                             result_dir=Path(doc.get("result_dir", "")),
                             log_dir=Path(doc.get("log_dir", "")),
                             head_job_stdout=Path(doc.get("head_job_stdout", "")),
                             head_job_stderr=Path(doc.get("head_job_stderr", "")),
                             head_job_exitcode_f=Path(doc.get("head_job_exitcode_f", ""))
                            )

        return task_data

    def to_dict(
                self
               ) -> Dict[str, Any]:
        """
        Преобразует объект TaskData в словарь для сохранения в MongoDB.

        Конвертирует все объекты Path в строки в формате POSIX
        и множества путей в списки строк.

        :return: Словарь с сериализованными данными.
        """
        dict_obj = self.__dict__
        for key in dict_obj:

            if key == "input_data":
                if dict_obj[key]:
                    dict_obj[key] = {
                                     group_name:[f.as_posix() for f in files]
                                     for group_name, files in dict_obj[key].items()
                                    }
                else:
                    dict_obj[key] = {}
            
            if key == "output_data":
                if dict_obj[key]:
                    dict_obj[key] = {
                                     group_name:{
                                                 file_group:[f.as_posix() for f in files]
                                                 for file_group, files in group_data.items()
                                                } for group_name, group_data in dict_obj[key].items()
                                    } 
                else:
                    dict_obj[key] = {}

            if key in [
                       'start_script',
                       'work_dir',
                       'result_dir',
                       'log_dir',
                       'head_job_stdout',
                       'head_job_stderr',
                       'head_job_exitcode_f'
                      ]:
                if dict_obj[key]:
                    dict_obj[key] = dict_obj[key].as_posix()
                else:
                    dict_obj[key] = ''
        return dict_obj

    def _check_output_files(
                            self
                           ) -> None:
        """
        Осуществляет поиск ожидаемых выходных файлов в директории результатов.

        Использует маски из `expected_output_data` для поиска файлов с помощью `Path.rglob`.
        В найденные файлы включаются только существующие файлы с ненулевым размером.
        Результаты поиска сохраняются в `output_data`. Размер найденных файлов суммируется в `output_files_size`.

        Логирует информацию о найденных и отсутствующих файлах.
        Если директория результатов не существует — записывает ошибку в лог.
        """
        logger.debug(f"Поиск выходных файлов в папке {self.result_dir.as_posix()}")
        if self.result_dir.exists():
            for file_group, filetypes in self.expected_output_data.items():
                self.output_data[file_group] = {}
                for filetype, file_mask in filetypes.items():
                    # проводим поиск файлов по маске, добавляем только файлы с НЕНУЛЕВЫМИ размерами
                    found_files = [f for f in self.result_dir.rglob(file_mask)]
                    files = set()
                    for file in found_files:
                        filesize = file.stat().st_size
                        if all([
                                file.is_file(),
                                filesize
                               ]):
                            files.add(file)
                            self.output_files_size += filesize
                    if files:
                        logger.debug(f"Найдено файлов группы {file_group}: {len(files)}")
                    else:
                        logger.error(f"Файлы группы не {file_group} найдены")
                    self.output_data[file_group].update({filetype:files})
        else:
            logger.error(f"Папка результата {self.result_dir.as_posix()} не найдена.")
        return None
