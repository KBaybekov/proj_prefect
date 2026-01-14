# tests/unit/test_task_data.py
import pytest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path
from modules.scheduler.classes.task_data import TaskData


class TestTaskData:
    """Тесты для класса TaskData."""

    def test_initialization_default_values(self):
        """Проверяет, что все поля инициализируются с значениями по умолчанию."""
        task_data = TaskData()

        assert task_data.input_data == {}
        assert task_data.input_files_size == 0
        assert task_data.expected_output_data == {}
        assert task_data.output_data == {}
        assert task_data.output_files_size == 0
        assert task_data.start_script == Path()
        assert task_data.work_dir == Path()
        assert task_data.result_dir == Path()
        assert task_data.log_dir == Path()
        assert task_data.head_job_stdout == Path()
        assert task_data.head_job_stderr == Path()
        assert task_data.head_job_exitcode_f == Path()

    def test_from_dict_with_valid_data(self):
        """Проверяет корректную десериализацию из словаря."""
        doc = {
            "input_data": {
                "fast5": ["/data/sample.fast5"]
            },
            "expected_output_data": {
                "QC": {"html": "*.html"}
            },
            "output_data": {
                "QC": {"html": ["/results/qc.html"]}
            },
            "input_files_size": 1024,
            "output_files_size": 2048,
            "start_script": "/scripts/start.sh",
            "work_dir": "/work/sample",
            "result_dir": "/results",
            "log_dir": "/results/logs",
            "head_job_stdout": "/logs/slurm.out",
            "head_job_stderr": "/logs/slurm.err",
            "head_job_exitcode_f": "/work/sample.exitcode"
        }

        task_data = TaskData.from_dict(doc)

        # Проверяем преобразование строк в Path
        assert task_data.work_dir == Path("/work/sample")
        assert task_data.start_script == Path("/scripts/start.sh")
        assert isinstance(task_data.input_data["fast5"], set)
        assert Path("/data/sample.fast5") in task_data.input_data["fast5"]
        assert task_data.expected_output_data == {"QC": {"html": "*.html"}}
        assert Path("/results/qc.html") in task_data.output_data["QC"]["html"]

    def test_from_dict_with_empty_input(self):
        """Проверяет поведение при пустом входном словаре."""
        task_data = TaskData.from_dict({})

        assert task_data.input_data == {}
        assert task_data.expected_output_data == {}
        assert task_data.output_data == {}
        assert task_data.input_files_size == 0
        assert task_data.output_files_size == 0
        assert task_data.work_dir == Path()
        assert task_data.start_script == Path()

    def test_to_dict_serializes_paths_correctly(self):
        """Проверяет корректную сериализацию объектов Path в строки."""
        task_data = TaskData(
            input_data={"fast5": {Path("/data/sample.fast5")}},
            expected_output_data={"QC": {"html": "*.html"}},
            output_data={"QC": {"html": {Path("/results/qc.html")}}},
            input_files_size=1024,
            output_files_size=512,
            work_dir=Path("/work/sample"),
            result_dir=Path("/results"),
            start_script=Path("/scripts/start.sh")
        )

        result = task_data.to_dict()

        assert result["work_dir"] == "/work/sample"
        assert result["start_script"] == "/scripts/start.sh"
        assert result["input_data"]["fast5"] == ["/data/sample.fast5"]
        assert result["output_data"]["QC"]["html"] == ["/results/qc.html"]
        assert result["expected_output_data"] == {"QC": {"html": "*.html"}}

    def test_to_dict_with_empty_fields(self):
        """Проверяет сериализацию при отсутствующих данных."""
        task_data = TaskData()

        result = task_data.to_dict()

        assert result["input_data"] == {}
        assert result["output_data"] == {}
        assert result["work_dir"] == ""
        assert result["result_dir"] == ""

    @patch("modules.scheduler.classes.task_data.logger")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.rglob")
    def test_check_output_files_directory_not_exists(
        self, mock_rglob, mock_exists, mock_logger
    ):
        """Проверяет логирование ошибки, если директория результатов не существует."""
        mock_exists.return_value = False
        task_data = TaskData(result_dir=Path("/results"))

        task_data._check_output_files()

        mock_logger.error.assert_called_once_with(
            "Папка результата /results не найдена."
        )

    @patch("modules.scheduler.classes.task_data.logger")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.rglob")
    def test_check_output_files_no_files_found(
        self, mock_rglob, mock_exists, mock_logger
    ):
        """Проверяет поведение, когда файлы по маске не найдены."""
        mock_exists.return_value = True
        mock_rglob.return_value = []

        task_data = TaskData(
            result_dir=Path("/results"),
            expected_output_data={"QC": {"html": "*.html"}}
        )

        task_data._check_output_files()

        assert "QC" in task_data.output_data
        assert "html" in task_data.output_data["QC"]
        assert task_data.output_data["QC"]["html"] == set()
        mock_logger.error.assert_called_with(
            "Файлы группы не QC найдены"
        )

    @patch("modules.scheduler.classes.task_data.logger")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.rglob")
    def test_check_output_files_finds_valid_files(
        self, mock_rglob, mock_exists, mock_logger
    ):
        """Проверяет корректный поиск существующих файлов с ненулевым размером."""
        mock_exists.return_value = True

        # Создаём мок-файлы
        mock_file1 = MagicMock(spec=Path)
        mock_file1.is_file.return_value = True
        mock_file1.stat().st_size = 1024
        mock_file1.__str__.return_value = "/results/qc.html"

        mock_rglob.return_value = [mock_file1]

        task_data = TaskData(
            result_dir=Path("/results"),
            expected_output_data={"QC": {"html": "*.html"}}
        )

        task_data._check_output_files()

        assert mock_file1 in task_data.output_data["QC"]["html"]
        assert task_data.output_files_size == 1024
        mock_logger.debug.assert_any_call(
            "Найдено файлов группы QC: 1"
        )

    @patch("modules.scheduler.classes.task_data.logger")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.rglob")
    def test_check_output_files_skips_empty_files(
        self, mock_rglob, mock_exists, mock_logger
    ):
        """Проверяет, что файлы с нулевым размером игнорируются."""
        mock_exists.return_value = True

        # Мок-файл с нулевым размером
        mock_file = MagicMock(spec=Path)
        mock_file.is_file.return_value = True
        mock_file.stat().st_size = 0

        mock_rglob.return_value = [mock_file]

        task_data = TaskData(
            result_dir=Path("/results"),
            expected_output_data={"QC": {"html": "*.html"}}
        )

        task_data._check_output_files()

        assert task_data.output_data["QC"]["html"] == set()
        assert task_data.output_files_size == 0
        mock_logger.error.assert_called_with(
            "Файлы группы не QC найдены"
        )
