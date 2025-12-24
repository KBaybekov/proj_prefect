from yaml import dump
from pathlib import Path

def save_yaml(filename:str, path:str, data:dict) -> Path:
    """
    Сохраняет словарь в YAML-файл.

    Использует pyyaml для сериализации данных в читаемый формат.
    Отключает сортировку ключей и использует блочный стиль.

    :param filename: Имя файла без расширения (например, 'config').
    :type filename: str
    :param path: Путь к директории, где будет сохранён файл (должен включать '/' в конце).
    :type path: str
    :param data: Словарь с данными для сохранения.
    :type data: dict
    :return: Полный путь к созданному YAML-файлу.
    :rtype: str
    """
    # Полный путь к файлу
    file_path = (Path(path) / f'{filename}.yaml').resolve()

    # Записываем данные в YAML-файл
    with open(file_path, 'w') as yaml_file:
        dump(data, yaml_file, default_flow_style=False, sort_keys=False)
    return file_path

def test_save_yaml():
    data = {"a":"b"}
    assert save_yaml(
                     filename='bb',
                     path='tmp',
                     data=data
                    ).as_posix() == "/raid/kbajbekov/common_share/github/proj_prefect/tmp/bb.yaml"