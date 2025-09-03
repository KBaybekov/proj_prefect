import os
from utils.common import log_error
from utils.nanopore import DirMeta, SourceFileMeta, BatchMeta, SampleMeta, upsert_dirs, upsert_batch, upsert_samples, generate_final_fingerprint
from pathlib import Path
import logging
import importlib.util
from typing import Any, Iterable, List, Tuple, Dict, Set, Union


logger = logging.getLogger(__name__)  # наследует конфиг из watchdog.py


def check_fs_ok(
                in_d:str,
                link_d:str,
                out_d:str,
                tmp_d:str
                ) -> tuple:
    """
    Проверяет существование и доступность необходимых директорий.
    Возвращает списки файлов и адреса директорий в случае успеха,
    иначе - завершает работу с ошибкой.
    ---------
    :returns tuple: Директория с входными данными, директория для результатов, директория для временных файлов
    """
    dirs = (in_d, link_d, out_d, tmp_d)

    # Проверяем,чтобы были указаны разные папки
    if len(set(dirs)) != 3:
        log_error(
                  error_type=NameError,
                  error_message='Должны быть указаны разные папки для входных и выходных данных'
                  )
    checked_dirs = []
    for i, dir in enumerate(dirs):
        # Проверяем, папки ли это вообще
        if not os.path.isdir(dir):
            log_error(
                      error_type=NotADirectoryError,
                      error_message=f"Не является папкой:\n{dir}"
                      )
        # Проверяем права доступа
        # Для папки с входными данными нам нужны только права на чтение
        if not os.access(path=dir, mode=os.R_OK): # pyright: ignore[reportArgumentType]
            log_error(
                      error_type=PermissionError,
                      error_message=f'Отсутствуют права на чтение для папки {dir}'
                      )
        # Для остальных папок нужны права на запись
        if i != 0:
            if not os.access(path=dir, mode=os.W_OK): # pyright: ignore[reportArgumentType]
                log_error(
                      error_type=PermissionError,
                      error_message=f'Отсутствуют права на запись для папки {dir}'
                      )
        # Конвертируем в Path-объект
        checked_dirs.append(Path(dir).resolve())
    return tuple(checked_dirs)


def index_source_files(
                       source_dir:Path,
                       link_dir:Path,
                       filetypes:tuple[str, ...],
                       dir_snapshots:dict
                       ) -> List[Dict[str, Any]]:
    """
    Делает первичный обход папок с исходными данными и симлинками к ним,
    собирает метаданные всех файлов с указанными расширениями в виде списка SourceFileMeta.
    В случае отсутствия симлинка на исходный файл - создаёт его
    """


    """
    !!! Доделать:
    Нужно сделать логику сохранения метаданных о папках, относящихся к файлам/батчам/образцам.
    На 03.09.2025 мне это видится так:
    - при сборе метадаты файлов мы вычленяем из имени файла папку (nanopore.py:upsert_dirs())
    - из имени папки мы вычленяем все папки от source_dir до конечной папки включительно (nanopore.py:upsert_dirs())
    - для каждой папки формируем метадату, включающую информацию о файлах, размере, родительской папке и подпапках (nanopore.py:upsert_dirs(),
                                                                                                                    nanopore.py:DirMeta)
    - дальше, каким-то образом, при инициализации нам нужно максимально экономично выявить и локализовать изменения в файлах, на основе метадаты папок
      (для обхода по дереву мы и сформировали свойства parent/kids для каждой из папок). Проблема в том, что я не могу придумать, как это сделать экономично
    - также нужно придумать, как ловить новые папки/файлы в source_dir. Возможно, подсмотреть, как это реализовано в вотчдоге от Миши?
    """
    summary_files: Dict[str, Set[Path]]
    dirs: Dict[str, DirMeta] = {}
    files: Dict[str, SourceFileMeta] = {}
    batches: Dict[str, BatchMeta] = {}
    samples: Dict[str, SampleMeta] = {}
    index: List[dict] = []
        
    source_files = get_files_in_dir_tree(
                                         dirs=[source_dir],
                                         extensions=filetypes,
                                         )
    summary_files = {}
    for summary in ['final_summary', 'sequencing_summary']:
        summary_files[summary] = set(source_dir.glob(f'*/*/*{summary}*.txt'))
    
    for filetype in filetypes:
        # Находим все файлы указанного типа в списке исходных файлов
        filetype_files = [file for file in source_files if file.name.endswith(filetype)]
        if filetype_files:
            # Создаем целевую директорию для ссылок на файлы этого типа
            target_filetype_dir = link_dir / filetype if '.' not in filetype \
                                                else link_dir / filetype.split('.')[0]
            target_filetype_dir.mkdir(parents=True, exist_ok=True)

            for file in filetype_files:
                meta_file, files, dirs = form_file_metadata(source_dir, file, target_filetype_dir, files, dirs)
                batches = form_batch_metadata(meta_file, batches, summary_files)


                
    logger.info("Metadata for source files and batches gathered, symlinks created.")
    # Собрав данные об исходных файлах и батчах, заполняем информацию по образцам
    for meta_batch in batches.values():
        # Создаём отпечатки для каждого батча
        meta_batch.fingerprint = generate_final_fingerprint(raw_fingerprint=meta_batch._fingerprint)
        # Создаём метадату образцов
        samples = form_sample_metadata(meta_batch, samples, files)        
    
    for meta_sample in samples.values():
        if not meta_sample.needs_curation:
            meta_sample.fingerprint = generate_final_fingerprint(raw_fingerprint=meta_sample._fingerprint)
    index = [files, dirs, batches, samples]
    return index


def form_file_metadata(
                       source_d:Path,
                       file:Path,
                       target_filetype_dir:Path,
                       files:Dict[str, SourceFileMeta],
                       dirs:Dict[str, DirMeta],
                      ) -> tuple:
    # Получаем метаданные файла
    meta_file = SourceFileMeta(filepath=file,
                                symlink_dir=target_filetype_dir)
    # Создаём ссылку, если она отсутствует в папке для ссылок
    if not meta_file.symlink.exists():
        create_symlink(file, meta_file.symlink)
    # В качестве id файла будет выступать имя его симлинка
    files.update({meta_file.symlink.as_posix():meta_file})
    # Создаём/обновляем мету директории файла
    dirs = upsert_dirs(source_d, dirs, meta_file)
    return (meta_file, files, dirs)


def form_batch_metadata(meta_file:SourceFileMeta,
                        batches: Dict[str, BatchMeta],
                        summary_files: Dict[str, Set[Path]]
                       ) -> Dict[str, BatchMeta]:
    # Добавляем записи о батче и образце, к которым относится файл
    # В этой же функции батч/образец добавляются в соответствующие списки
    batches = upsert_batch(
                            batches=batches,
                            src=meta_file,
                            summaries=summary_files
                            )
    return batches


def form_sample_metadata(meta_batch:BatchMeta,
                        samples: Dict[str, SampleMeta],
                        files: Dict[str, SourceFileMeta]
                       ) -> Dict[str, SampleMeta]:
    samples = upsert_samples(samples=samples,
                             src=meta_batch,
                             files_meta=files)
    return samples


def is_module_installed(module_name):
    """
    Проверяет, установлен ли модуль Python

    :param module_name (str): Название модуля
    :return: (bool) True если модуль установлен, иначе False.
    """
    spec = importlib.util.find_spec(module_name)
    return spec is not None


def fs_object_exists(path:str) -> bool:
    return Path(path).exists()


def remove_file(filepath: Union[str, os.PathLike], missing_ok: bool = True) -> bool:
    """
    Безопасно удаляет файл, обрабатывая возможные ошибки.

    :param filepath: Путь к файлу для удаления (строка или Path-объект)
    :param missing_ok: Если True, не считает отсутствие файла ошибкой (аналог `exist_ok` в Path.unlink())
    :return: True, если файл удален или отсутствовал (при missing_ok=True), False при ошибках удаления
    :raises TypeError: Если передан некорректный тип пути
    """
    try:
        os.remove(filepath)
        return True
    except FileNotFoundError:
        return missing_ok  # Возвращает True, если файла нет и это разрешено
    except PermissionError:
        print(f"Ошибка: Нет прав на удаление файла {filepath}")
        return False
    except IsADirectoryError:
        print(f"Ошибка: {filepath} является директорией (используйте shutil.rmtree())")
        return False
    except Exception as e:
        print(f"Не удалось удалить файл {filepath}: {str(e)}")
        return False


def create_symlink(file: Path, dest: Path, overwrite: bool = False) -> bool: # pyright: ignore[reportReturnType]
    """
    Создает символическую ссылку (symlink) из файла в указанное место.

    :param file: Исходный файл/директория, на который создается ссылка
    :param dest: Путь, где будет создана ссылка
    :param overwrite: Перезаписать существующую ссылку (False по умолчанию)
    :return: True если ссылка создана успешно, False в случае ошибки
    :raises RuntimeError: Если исходный файл не существует
    """
    try:
        # Проверка существования исходного файла
        if not file.exists():
            raise RuntimeError(f"Исходный файл {file} не существует")

        # Проверка существования целевой ссылки
        if dest.exists():
            if overwrite:
                dest.unlink()  # Удаляем существующую ссылку/файл
            else:
                logger.warning(f"Ссылка {dest} уже существует (используйте overwrite=True)")
                return False

        # Создаем родительские директории если нужно
        dest.parent.mkdir(parents=True, exist_ok=True)

        # Создаем символическую ссылку
        dest.symlink_to(file.resolve())
        logger.info(f"Создана ссылка: {dest} -> {file}")
        return True
    
    except OSError:
        log_error(
                  error_type=OSError,
                  error_message=f"Ошибка создания ссылки {dest}"
                  )
    except Exception:
        log_error(
                  error_type=Exception,
                  error_message=f"Неожиданная ошибка"
                  )
    

def create_folder(
    path: Union[str, List[str]],
    exist_ok: bool = True,
    parents: bool = False,
    verbose: bool = False
) -> bool:
    """
    Создает папку (или несколько папок) по указанному пути.

    :param path: Путь к папке (строка или список строк для создания нескольких папок).
    :param exist_ok: Если True, не вызывает ошибку при существовании папки (по умолчанию True).
    :param parents: Если True, создает все родительские папки (аналог `mkdir -p` в Linux).
    :param verbose: Если True, выводит сообщения о создании папок.
    :returns: True, если папка(и) создана успешно, иначе False.
    :raises OSError: Если папка не может быть создана (и exist_ok=False).
    """
    def _create_single_folder(folder_path: str) -> bool:
        try:
            os.makedirs(folder_path, exist_ok=exist_ok)
            if verbose:
                print(f"Папка создана: {folder_path}")
            return True
        except OSError as e:
            if verbose:
                print(f"Ошибка при создании папки {folder_path}: {e}")
            return False

    if isinstance(path, list):
        return all(_create_single_folder(p) for p in path)
    else:
        return _create_single_folder(path)


def get_samples_in_dir(dir:str, extensions:tuple, empty_ok:bool=False):
    """
    Генерирует список файлов на основе включающих и исключающих образцов.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :param extensions: Расширения файлов для поиска.
    :return: Список путей к файлам.
    """
    # Ищем все файлы в директории с указанными расширениями
    files = [os.path.join(dir, s) for s in os.listdir(dir) if s.endswith(extensions)]
    if not files and not empty_ok:
        raise FileNotFoundError("Образцы не найдены. Проверьте входные и исключаемые образцы, а также директорию с исходными файлами.")
    return files


def get_dirs_in_dir(dir:str, empty_ok:bool=False):
    """
    Генерирует список подпапок в указанной папке.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :return: Список путей к файлам.
    """
    # Ищем все файлы в директории с указанными расширениями
    dirs = [os.path.join(dir, s, '') for s in os.listdir(dir) if os.path.isdir(f'{dir}{s}')]
    if not dirs:
        raise FileNotFoundError("Образцы не найдены. Проверьте входные и исключаемые образцы, а также директорию с исходными файлами.")
    return dirs


def get_files_in_dir_tree(
    dirs: List[Path],
    extensions: tuple,
    empty_ok: bool = False,
    exclude_dirs: Tuple[Path, ...] = (Path('.git'), Path('__pycache__'))
) -> List[Path]:
    """
    Обходит дерево каталогов, корнями которого являются элементы `dirs`, и собирает список файлов,
    чьи имена оканчиваются на одно из значений из `extensions`. Обход выполняется в один проход
    без нормализации расширений и без глобального игнора скрытых директорий.

    Важно:
      • Исключаются только те каталоги, чьи ИМЕНА явно перечислены в `exclude_dirs` (сравнение по basename).
      • По симлинкам НЕ следуем (follow_symlinks=False), чтобы избегать потенциальных циклов.
      • Если `extensions` пустой кортеж, возвращаются все файлы.
      • Ошибки доступа/удаления веток во время обхода игнорируются для устойчивости.

    :param dirs: Список корневых директорий для обхода.
    :param extensions: Кортеж расширений для фильтрации. Используется буквально в str.endswith().
                       Никакой нормализации (регистр, точки) не выполняется.
    :param empty_ok: Если False и найденных файлов нет — возбуждается FileNotFoundError.
    :param exclude_dirs: Кортеж каталогов, которые нужно исключить по ИМЕНИ (basename), где бы они ни встретились.
    :return: Список путей к найденным файлам (List[Path]).
    """
    # Валидация корневых директорий.
    if not dirs:
        log_error(
                  error_type=ValueError,
                  error_message="Список корневых директорий пуст."
                  )
    for d in dirs:
        if not d.is_dir():
            log_error(
                      error_type=NotADirectoryError,
                      error_message=f"Не директория: {d}"
                      )

    # Преобразуем список исключаемых каталогов в набор ИМЁН (basename),
    # чтобы исключать такие каталоги в любом месте дерева.
    excluded_names = {p.name for p in exclude_dirs} if exclude_dirs else set()

    found_files: List[Path] = []
    # Используем стек для итеративного обхода (без рекурсии) — экономнее по памяти на глубоких деревьях.
    stack: List[Path] = list(dirs)

    while stack:
        current = stack.pop()
        try:
            # os.scandir быстрее Path.iterdir и даёт типы без лишних stat-вызовов.
            with os.scandir(current) as it:
                for entry in it:
                    try:
                        # Обрабатываем каталоги: не следуем по симлинкам.
                        if entry.is_dir(follow_symlinks=False):
                            # Исключаем ТОЛЬКО явно указанные имена; скрытые директории НЕ игнорируются автоматически.
                            if entry.name in excluded_names:
                                continue
                            stack.append(Path(entry.path))
                            continue

                        # Обрабатываем файлы: учитываем только реальные файлы (не симлинки).
                        if entry.is_file(follow_symlinks=False):
                            name = entry.name
                            # Если список расширений пуст — принимаем все файлы.
                            if not extensions or name.endswith(extensions):
                                found_files.append(Path(entry.path))

                        # Иные типы (симлинки, сокеты, спецфайлы) опускаем.
                    except FileNotFoundError:
                        # Объект мог исчезнуть между scandir и проверкой — пропускаем.
                        continue
        except (PermissionError, FileNotFoundError):
            # Нет прав на каталог или он исчез в процессе — пропускаем ветку.
            continue

    if not found_files and not empty_ok:
        log_error(
                  error_type=FileNotFoundError,
                  error_message="Файлы не найдены. Проверьте входные директории, фильтры расширений и исключения каталогов."
                  )
    return found_files


def get_filesize(file:str) -> int:
    """
    Возвращает размер файла в байтах
    ---------
    :param str: Абсолютный путь к файлу
    :returns int: Размер файла в байтах
    """
    return Path(file).stat().st_size


def humanize_filesize(size_bytes:float) -> str:
    """
    Возвращает размер файла в удобочитаемом формате
    ---------
    :param float: Размер файла в байтах
    :returns str: Размер файла и размерность
    """
    for unit in ['b', 'Kb', 'Mb', 'Gb', 'Tb']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} Pb"


def basename(file:str) -> str:
    """
    Возвращает имя файла
    ---------
    :param str: Путь файла
    :returns str: Базовое имя файла
    """
    return Path(file).name

def get_sample_ids_from_filenames(files:list, splitter:str, name_part:int) -> list:
    ids = []
    for file in files:
        ids.append(Path(file).name.split(splitter)[name_part])
    return ids
