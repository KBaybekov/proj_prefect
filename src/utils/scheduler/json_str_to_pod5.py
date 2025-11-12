import subprocess
from ast import literal_eval
from typing import Union, List, Dict, Optional, IO
from shlex import split as shlex_split
import json
import yaml



def run_subprocess(
                    cmd: Union[str, List[str]],
                    check: bool = False,
                    timeout: Optional[float] = None,
                    capture_output: bool = True,
                    env: Optional[Dict[str, str]] = None,
                    cwd: Optional[str] = None,
                    stdin: Optional[Union[IO, int]] = None,
                    stdout: Optional[Union[IO, int]] = None,
                    stderr: Optional[Union[IO, int]] = None,
                    input: Optional[str] = None,
                    text: bool = True,
                    shell: bool = False,
                    log_output: bool = True,
                    suppress_output: bool = False,
                    ) -> subprocess.CompletedProcess:
    """
    Обёртка над subprocess.run с расширенной логикой и безопасностью.

    :param cmd: Команда как строка или список аргументов.
    :param check: Если True, вызывает CalledProcessError при неудачном коде возврата.
    :param timeout: Таймаут выполнения в секундах.
    :param capture_output: Ловить ли stdout и stderr.
    :param env: Словарь переменных окружения.
    :param cwd: Рабочая директория.
    :param stdin: Входной поток.
    :param stdout: Выходной поток.
    :param stderr: Поток ошибок.
    :param input: Входные данные для stdin.
    :param text: Использовать текстовый режим.
    :param shell: Использовать shell (опасно!).
    :param log_output: Логировать вывод команды.
    :param suppress_output: Подавлять вывод в консоль.
    :return: Объект subprocess.CompletedProcess.
    """

    # Преобразование строки в список (если shell=False)
    if isinstance(cmd, str) and not shell:
        try:
            cmd = shlex_split(
                                cmd,
                                comments=False,
                                posix=True
                                )
        except ValueError as e:
            #print(f"Ошибка разбора команды '{cmd}': {e}")
            raise

    # Логирование команды
        #print(f"Запуск команды: {' '.join(cmd)}")

    # Настройка перенаправления вывода
    if suppress_output:
        stdout = subprocess.DEVNULL
        stderr = subprocess.DEVNULL
    elif capture_output:
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE

    try:
        result = subprocess.run(
                                cmd,
                                check=check,
                                timeout=timeout,
                                env=env,
                                cwd=cwd,
                                stdin=stdin,
                                stdout=stdout,
                                stderr=stderr,
                                input=input,
                                text=text,
                                shell=shell,
                                )
    except subprocess.CalledProcessError as e:
        #print(f"Ошибка выполнения команды: {' '.join(e.cmd)}")
        #print(f"Код возврата: {e.returncode}")
        #if e.stdout:
            #print("STDOUT:")
            #print(e.stdout)
        if e.stderr:
            #print("STDERR:")
            #print(e.stderr)
            raise
    except subprocess.TimeoutExpired as e:
        #print(f"Таймаут команды: {' '.join(e.cmd)}")
        #print(f"Время ожидания: {timeout}s")
        raise
    except Exception as e:
        #print(f"Неожиданная ошибка: {e}")
        raise

    # Логирование результата
    #if log_output and capture_output:
        ##print(f"STDOUT:\n{result.stdout}")
        #print(f"STDERR:\n{result.stderr}")

    return result

result = (run_subprocess(
                                   cmd=[
                                        "squeue",
                                        "--user", 'kbajbekov',
                                        "--json"
                                   ]
                                  ).stdout)
#print(result)
result = json.loads(result)

# Записываем в yaml словарь
if result:
    with open('tmp/slurm_tasks.yaml', 'w') as file:
        yaml.dump(result, file)
