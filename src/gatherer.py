from dotenv import load_dotenv
from utils.common import env_var
from utils.db import check_db_connection, update_db
from utils.filesystem import create_ont_data_symlinks, create_ont_summary_symlinks
from utils.nanopore import gather_ont_metadata
import time

load_dotenv()

if __name__ == '__main__':
    source_folder = env_var('DATA_G_SOURCE_DIR')
    target_folder = env_var('DATA_G_LINK_DIR')
    sleep_time = int(env_var('DATA_G_SLEEP_SEC'))
    file_extensions = env_var('SOURCE_FILES_EXTENSIONS').split(',')

    while True:
        # Создание симлинков для данных секвенирования
        batches, samples = create_ont_data_symlinks(source_folder,
                                                    target_folder,
                                                    file_extensions)
        # Создание симлинков для summary запусков
        create_ont_summary_symlinks(source_folder,
                                     target_folder)
        print('Symlinks created')
        # Собираем метадату запусков и список исходников
        print('Gathering metadata...')
        source_files, joint_batch_metadata = gather_ont_metadata(target_folder, batches, samples, 'data/pores_n_chemistry.yaml')
        print('Metadata gathered')

        # Вносим записи об обнаруженных образцах в БД
        db = check_db_connection()
        # Вносим новые образцы в БД
        update_db(db=db, files=source_files, metadata=joint_batch_metadata)
        
        # Очищаем кэш
        del batches
        del samples
        del source_files
        del joint_batch_metadata

        print('Going sleep...')
        time.sleep(sleep_time)