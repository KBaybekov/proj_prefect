import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Папка для наблюдения
WATCHED_FOLDER = "/common_share/github/proj_prefect/tmp/test"

def long_function_creation(file_path):
    """Пример длительной операции при создании файла"""
    print(f"[THREAD] Запущена обработка нового файла: {file_path}")
    time.sleep(5)  # Имитация долгой задачи
    print(f"[THREAD] Обработка завершена для: {file_path}")

def long_function_modification(file_path):
    """Пример длительной операции при изменении файла"""
    print(f"[THREAD] Запущена обработка изменения файла: {file_path}")
    time.sleep(3)  # Имитация долгой задачи
    print(f"[THREAD] Обработка изменений завершена для: {file_path}")

class FileChangeHandler(FileSystemEventHandler):
    def on_created(self, event):
        print(event.__dict__)
        if not event.is_directory:
            threading.Thread(
                target=long_function_creation,
                args=(event.src_path,),
                daemon=True
            ).start()

    def on_modified(self, event):
        print(event.__dict__)
        if not event.is_directory:
            threading.Thread(
                target=long_function_modification,
                args=(event.src_path,),
                daemon=True
            ).start()

    def on_deleted(self, event):
        print(event.__dict__)

    def on_moved(self, event):
        print(event.__dict__)

if __name__ == "__main__":
    # Создаем наблюдателя
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path=WATCHED_FOLDER, recursive=False)
    
    # Запуск наблюдателя в отдельном потоке
    observer.start()
    print(f"Наблюдение за папкой: {WATCHED_FOLDER}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
