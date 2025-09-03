
"""
_______________________________________________________
DEPRECATED
_______________________________________________________
"""

from utils.common import run_shell_cmd
from utils.filesystem import get_samples_in_dir, create_folder, basename, fs_object_exists,\
                             get_filesize, humanize_filesize, load_yaml, save_yaml, remove_file
import re
import logging
from utils.filesystem import load_yaml
from pathlib import Path

logger = logging.getLogger(__name__)  # наследует конфиг из watchdog.py


def create_ont_data_symlinks(source_dir:str, target_dir:str, filetypes:list) -> tuple:
    """
    Ищет исходные файлы секвенирования согласно стандартной структуре папки образца.
    Создаёт новое имя для ссылки (батч:образец:id_файла), переформатируя путь исходника.
    Возвращает кортеж батчей и образцов
    :param source_dir: Полный путь директории с образцами
    :param target_dir: Полный путь директории для создания ссылок
    :param filetype: Расширение фай
    :return tuple: кортеж множеств батчей и образцов
    """
    source_dir = Path(source_dir)  # type: ignore
    target_dir = Path(target_dir)# type: ignore
    batches = []
    samples = []

    print("Creating data symlinks...", end='')    
    for filetype in filetypes:
        # Находим все файлы указанного типа в исходной структуре
        files = source_dir.glob(f'*/*/*/*.{filetype}')# type: ignore
        if files:
            # Создаем целевую директорию
            target_filetype_dir = target_dir / filetype if '.' not in filetype \
                                                else target_dir / filetype.split('.')[0]
            
            target_filetype_dir.mkdir(parents=True, exist_ok=True)

            for file in files:
                # Получаем части пути
                parts = file.parts
                # Имя запуска находится на позиции -3 в пути вместе с кодами даты и положения ячейки в приборе
                batch_name = '_'.join(str(parts[-3]).split('_')[3:])
                batches.append(batch_name)
                # Имя образца находится на позиции -4 в пути
                sample_name = parts[-4]
                samples.append(str(sample_name))
                
                # Создаем ссылку
                target_link = target_filetype_dir / f"{batch_name}:{sample_name}:{file.name}"
                if not target_link.exists():
                    create_symlink(file, target_link)# type: ignore
    print("done")
    return (set(batches), set(samples))


def create_ont_summary_symlinks(source_dir:str, target_dir:str):
    source_dir = Path(source_dir)# type: ignore
    target_dir = Path(target_dir)# type: ignore
    
    print("Creating summary symlinks...", end='') 
    # Находим все файлы указанного типа в исходной структуре
    for summary in ['final_summary', 'sequencing_summary']:
        files = source_dir.glob(f'*/*/*{summary}*.txt')# type: ignore
        if files:
            # Создаем целевую директорию
            target_summary_dir = target_dir / summary# type: ignore
            
            target_summary_dir.mkdir(parents=True, exist_ok=True)

            for file in files:
                # Создаем ссылку
                target_link = target_summary_dir / file.name
                if not target_link.exists():
                    create_symlink(file, target_link)# type: ignore
    print("done")



def _gather_ont_metadata(
                        dir:str,
                        batches:set,
                        samples:set,
                        pore_n_chem_data:str, 
                        model_data:str
                        ) -> tuple:
    """
    Ищет в указанной директории папки с данными секвенирования (pod5/, fast5/)
    и папку с final_summary (final_summary/), парсит данные и формирует .yaml
    со структурой:
    {батч: [список образцов], проточная ячейка, использованный кит,
           версия поры, частота сэмплинга, дата запуска, sequencing_summary}

    :param dir: Директория с данными секвенирования и метадатой
    :param batches: Множество батчей
    :param pore_n_chem_data: YAML с данными по соответствию моделей ячейки и химии версии поры и другим параметрам
    :return tuple: список файлов POD5 и FAST5, метадату батчей
    """
    metadata_dirs = {'batch':f"{dir}/metadata/batches/",
                     'sample':f"{dir}/metadata/samples"}
    # Загружаем инфу о порах, химии и моделях
    pore_data = load_yaml(file_path=pore_n_chem_data, critical=True)
    models = load_yaml(file_path=model_data, critical=True)
    # Собираем список файлов с метадатой в конкретной папке, иначе - создаём эту папку
    yamls = {}
    for m_type, subdir in metadata_dirs.items():
        if fs_object_exists(subdir):
            yamls[m_type] = get_samples_in_dir(dir=subdir, extensions=('.yaml'), empty_ok=True) # type: ignore
        else:
            create_folder(subdir,
                          parents=True)
            yamls[m_type] = []
    batch_yamls = yamls['batch']
    sample_yamls = yamls['sample']
    
    # Собираем список интересующих файлов и информацию об их размере
    print('   Retrieving filelists...', end='')
    final_summaries = get_samples_in_dir(dir=f"{dir}/final_summary/", extensions='.txt', empty_ok=True) # type: ignore
    seq_summaries = get_samples_in_dir(dir=f"{dir}/sequencing_summary/", extensions='.txt', empty_ok=True) # type: ignore
    pod5s = get_samples_in_dir(dir=f"{dir}/pod5/", extensions='.pod5', empty_ok=True) # type: ignore
    fast5s = get_samples_in_dir(dir=f"{dir}/fast5/", extensions='.fast5', empty_ok=True) # type: ignore
    fqs = get_samples_in_dir(dir=f"{dir}/fastq/", extensions='.fastq.gz', empty_ok=True) # type: ignore
    print('done')
    
    # Получение метаданных для батчей
    
    joint_batch_metadata = form_batch_metadata(  # type: ignore
                                                batches,
                                                final_summaries,
                                                seq_summaries,
                                                pod5s,
                                                fast5s,
                                                fqs,
                                                dir,
                                                pore_data
                                                )                                           
    
    # Получение списка исходных файлов и их размеров
    source_files = {}
    for filetype in ['pod5', 'fast5', 'fq']:
            for flag in ['pass', 'fail']:
                source_files[f"{filetype}_{flag}"] = {}
    for _, metadata in joint_batch_metadata.items():
        for file_group in source_files.keys():
            source_files[file_group].update(metadata[file_group])

    # Получение метаданных для образцов
    joint_sample_metadata = form_sample_metadata( # type: ignore
                                                samples,
                                                batches,
                                                joint_batch_metadata,
                                                source_files
                                                )
    
    def form_batch_metadata(
                            batches:set,
                            final_summaries:list,
                            seq_summaries:list,
                            pod5s:list,
                            fast5s:list,
                            fqs:list,
                            dir:str,
                            pore_data:dict
                            ) -> dict: # type: ignore
        """
        Формирует словарь с метаданными батчей
        """


        # Метадата батчей будет собираться и в кэш, сделаем словарь со структурой {батч:метадата}
        joint_metadata = {batch:{} for batch in batches}
        print(f'  Found {len(batches)} batches...')
        # Если у батча нет метадаты, создаём её
        for batch in batches:
            print(f'  Processing batch: {batch}:')
            batch_yaml = next([y for y in batch_yamls
                                if batch in basename(y)], # type: ignore
                                '')
            if batch_yaml:
                # Проверяем, вся ли метадата для батча в наличии
                print("  YAML found. Parsing...", end='')
                batch_yaml = next(item for item in batch_yamls if batch in item)
                metadata = load_yaml(batch_yaml)
                # Если требуется обновление - вытягиваем данные из yaml с порами
                if "needs_update" in batch_yaml:
                    print(' Unknown items in metadata!')
                    # Удаляем старый файл, чтобы не мешался
                    remove_file(batch_yaml)
                    batch_yamls.remove(batch_yaml)

                    # Обновляем данные поры и химии, сохраняем в кэш и файл
                    for key, val in {metadata['cell']:'pore', metadata['kit']:'chemistry'}.items():
                        metadata[val] = pore_data[key]
                    filename = f"{batch_yaml.replace('needs_update', '')}" if \
                                            not any(['needs_update' in k for k in metadata.values()]) \
                                                else batch_yaml
                    if 'needs_update' in filename:
                        print('  Still unknown')
                        joint_metadata[batch] = metadata
                    else:
                        print('  Metadata gathering completed!')
                        print('    Defining basecalling models...', end='')
                        metadata = define_basecalling_models(metadata, models) # type: ignore
                        print('done')
                        joint_metadata[batch] = metadata

                    new_yaml = save_yaml(filename, f"{dir}/metadata/", metadata)
                    batch_yamls.append(new_yaml)
                    print('    Metadata saved')
                # В противном случае - просто кэшируем содержимое ямла
                else:
                    print("done.")
                    joint_metadata[batch] = metadata
                # Обновляем список неизвестных значений, убирая ненужное из файлов
                txts_to_update = get_samples_in_dir(dir, extensions=(',needs_update.txt')) # type: ignore
                for txt in txts_to_update:
                    with open(txt, mode='w+') as f:
                        # Проверяем, чтобы из списка были убраны элементы, значения для которых были записаны
                        # в pores_n_chemistry.yaml после последней проверки
                        unknown_vals = [item for item in f.readlines() if item not in pore_data.keys()]
                        unknown_vals.append(val) # type: ignore
                        f.truncate()
                        f.write('\n'.join(set(unknown_vals)))
            else:
                print(f'    Batch metadata not found. Parsing source file...', end='')
                # Берем один из файлов секвенирования батча, достаём метаданные 
                source_file = next((x for x in pod5s if x is not None), 
                                    next((x for x in fast5s if x is not None), 'no batch file found :('))
                metadata = parse_source_file_metadata(source_file) # type: ignore
                print('done')
                
                #Определяем версию химии
                print(f'    Forming metadata:')
                print(f'      Storing sequencer data...', end='')
                metadata['chemistry'] = pore_data.get(metadata['sequencing_kit'].upper(), 'unknown_chem,needs_update')
                
                metadata['final_summary'] = next((item for item in final_summaries if batch in item), 'unknown')
                metadata['sequencing_summary'] = next((item for item in seq_summaries if batch in item), 'unknown')
                print(f'done')

                # Записываем неизвестные ячейку/химию для ручной курации
                any_unknown = False
                for key, val in {
                                metadata['pore']:metadata['flow_cell'],
                                metadata['chemistry']:metadata['sequencing_kit'].upper()
                                }.items():
                    # Если 'needs_update' по какому-либо из параметров - вносим значение в файл-сборщик для ручной курации
                    if 'needs_update' in key:
                        any_unknown = True
                        print(f'    Found unknown vals!..')
                        store_file = f'{dir}/{key}.txt'
                        # Если файл уже есть, запихиваем в список новый элемент и сохраняем сетсписка , иначе - создаём с нуля
                        if fs_object_exists(store_file):
                            with open(store_file, mode='w+') as f:
                                # Проверяем, чтобы из списка были убраны элементы, значения для которых были записаны
                                # в pores_n_chemistry.yaml после последней проверки
                                unknown_vals = [item for item in f.readlines() if item not in pore_data.keys()]
                                unknown_vals.append(val)
                                f.truncate()
                                f.write('\n'.join(set(unknown_vals)))
                        else:
                            with open(store_file, mode='w') as f:
                                f.write(val)
                if not any_unknown:
                    # Если всё ок - определяем модель и список возможных модификаций,
                    # которые можно обнаружить при бэйсколлинге
                    print('    Defining basecalling models...', end='')
                    metadata = define_basecalling_models(metadata, models) # type: ignore
                    print('done')
                else:
                    metadata['basecalling_models'] = ''
                    metadata['mod_basecalling_models'] = ''
                
                # Получаем словарь вида {'pod5_pass':{'x_pass.pod5':1024}}
                print(f"      Storing files' properties...", end='')
                batch_source_files = {}
                for flag in ['pass', 'fail']:
                    for filetype, filelist in {'pod5':pod5s, 'fast5':fast5s, 'fq':fqs}.items():
                        batch_source_files[f"{filetype}_{flag}"] = {
                                                            file: get_filesize(file)
                                                            for file in filelist
                                                            if all([flag in basename(file),
                                                                    batch in basename(file)])
                                                            }
                # Суммируем размеры файлов для каждой группы
                for file_group, file_data in batch_source_files.items():
                    metadata[f"size_{file_group}"] = humanize_filesize(sum(file_data.values()))
                # Записываем список файлов для каждой группы
                for file_group, file_data in batch_source_files.items():
                    metadata[file_group] = file_data
                print(f'done')

                # Сохраняем полученные данные
                joint_metadata[batch] = metadata
                filename = f"{batch}" if \
                                not any(['needs_update' in k for k in metadata.values()]) \
                                    else f"{batch}_needs_update"
                # Добавляем готовый .yaml в список yaml-файлов
                new_yaml = save_yaml(filename, f"{dir}/metadata/", metadata)
                batch_yamls.append(new_yaml)
                print('    Metadata saved')

    def form_sample_metadata(
                             samples:set,
                             batches:set,
                             joint_batch_metadata:dict,
                             source_files:dict
                             ) -> dict: # type: ignore
        """
        Формирует словарь с метаданными образцов
        """
        joint_sample_metadata = {}
        print(f"Found {len(samples)} samples.")
        for sample in samples:
            # Ищем сохранённые метаданные
            sample_yaml = next([y for y in sample_yamls  # type: ignore
                                if sample in basename(y)],
                                '')
            if sample_yaml:
                sample_metadata = load_yaml

            else:
                sample_metadata = {}
                # Собираем все файлы, относящиеся к образцу
                sample_source_files = {k:[file for file in v
                                          if sample in basename(file)]
                                       for k,v in source_files.items()}
                # Сделаем плоский список для простой итерации
                sample_source_files_flat = []
                for filelist in sample_source_files.values():
                    sample_source_files_flat.extend(filelist)
                # Собираем наименования батчей, относящихся к образцу,
                # т.е. файлы батча есть в списке файлов образца
                sample_batch_files = {batch:[file for file in sample_source_files_flat
                                             if batch in basename(file)]
                                      for batch in batches}
                # Пробегаем по метаданным батчей, собираем всё, относящееся к образцу
                sample_source_files = {k:[file for file in v
                                          if sample in basename(file)]
                                       for k,v in source_files.items()}
                for batch, batch_metadata in joint_batch_metadata.items():
                    # Итерируем по ключам source_files, но в рамках батча
                    # Первоочередной ключ - тип эксперимента
                    if batch_metadata['experiment_type'] not in sample_metadata.keys():
                        sample_metadata[batch_metadata['experiment_type']] = {}
                    
                    # Итерируем 
                """ for file_type, files in source_files.items():
                    sample_source_files[file_type] = [f for f in files
                                                      if sample in basename(f)]
                batches_"""



    return (source_files, joint_batch_metadata, joint_sample_metadata)


def _define_basecalling_models(metadata:dict, models:dict) -> tuple:
    # Флаг для обозначения успешности определения моделей
    straight_model_defined = False
    mod_model_defined = False

    params2search = [
                     'experiment_type',
                     'sample_frequency',
                     'pore',
                     'chemistry'
                    ]
    # Отдельно вытащим скорость работы поры, чтобы дискриминировать и по ней, если понадобится
    pore_speed = metadata.get('pore_speed', '')

    # Постараемся определить модели для всех трёх степеней точности
    grouped_models = {'fast':[], 'hac':[], 'sup':[]}
    for accuracy_degree in grouped_models.keys():
        # Составляем список моделей с заданной точностью
        grouped_models[accuracy_degree] = [
                                           data for data in models.values()
                                           if accuracy_degree in data['model']
                                          ]
    # Для каждой группы выбираем наилучшую модель
    # как для обычного бейсколлинга, так и для бейсколлинга с модификациями
    result = {k:None for k in grouped_models.keys()}
    for group_type, models_in_group in grouped_models.items():
        if not models_in_group:
            continue
        # Фильтруем модели по критериям
        filtered_models = []
        for model in models_in_group:
            # Проводим определение подходящей модели, итерируя по признакам
            for param_idx, param in enumerate(params2search):
                key2search = 'model'
                val2search = metadata.get(params2search[param_idx], '')
                if val2search:
                    # если параметр "experiment_type", то оставляем последние три буквы (dna/rna)
                    if param == 'experiment_type':
                        val2search = val2search[-3:]
                    # если параметр "sample_frequency", то смотрим freq модели
                    elif param == 'sample_frequency':
                        key2search = 'freq'
                    # Модель проходит проверку - модель добавляется в список
                    if val2search not in model[key2search]:
                        continue
                    filtered_models.append(model)
        # Если хотя бы в одной модели совпадает скорость работы поры (и она не "") - фильтруем по этом признаку
        if len(filtered_models) > 1:
            if all([pore_speed,
                    any([pore_speed in model['model']
                         for model in filtered_models])]):
                
                filtered_models = [model for model in filtered_models
                                   if pore_speed in model['model']]
                
        # Если отфильтрованных моделей больше одной - выбираем самую продвинутую для обычного бэйсколлинга,
        # выделяя номер версии модели и сортируя список моделей по убыванию версий
        if len(filtered_models) > 1:
            filtered_models.sort(
                                key=lambda x:
                                    extract_model_version(x['model_version']), # type: ignore
                                    reverse=True
                                )
        best_model = filtered_models[0]['model']
        result[group_type] = best_model
    
    # Определяем, получилось ли найти хоть одну модель для бейсколлинга без модификаций
    if any([len(model)>0 for model in result.values()]): # pyright: ignore[reportArgumentType]
        straight_model_defined = True

    # Из группы sup выбираем как можно больше возможных модификаций,
    # которые можно набэйсколлить, и для каждой модификации определяем наиболее подходящую модель,
    # используя алгоритм (по убыванию значимости): номер версии модели, номер версии модели модификации 
    result_mod = {}
    for model in result['sup']: # type: ignore
        mod_versions = model['mod_data_versions']
        for mod in mod_versions.keys():
            if mod:  # Исключаем пустые модификации
                # Добавляем модификацию в результирующий словарь (если её там ещё нет),
                # а в её список - название модели
                if mod not in result_mod.keys():
                    result_mod[mod] = [model]
                else: result_mod[mod].append(model)
    # Повторяем процесс сортировки по версиям моделей для каждой модификации,
    # выбирая самую продвинутую
    for mod in result_mod.keys():
        if len(result_mod[mod]) > 1:
            result_mod[mod] = result_mod[mod].sort(
                                                key=lambda x:
                                                    extract_model_version(x['model_version']), # type: ignore
                                                    reverse=True
                                                )
        result_mod[mod] = result_mod[mod][0]['model']
    # Определяем, получилось ли найти хоть одну модель для бейсколлинга без модификаций
    if any([len(model)>0 for model in result_mod.values()]): # pyright: ignore[reportArgumentType]
        mod_model_defined = True
    
    return (straight_model_defined, mod_model_defined, result, result_mod) 


def _extract_model_version(model_name):
    # Ищем версию в формате vX.Y.Z или vX.Y
    version_match = re.search(r'@v([\d.]+)', model_name)
    if version_match:
        # Разбиваем версию на компоненты и преобразуем в числа
        version_parts = version_match.group(1).split('.')
        return tuple(int(part) for part in version_parts)
    return (0,)  # Возвращаем кортеж с одним нулем для сравнения


def _parse_final_summary(file:str) -> tuple:
    with open(file, 'r') as f:
        data = f.readlines()

        equipment_row = next(row for row in data if 'protocol=' in row)
        cell = equipment_row.split(':')[-2]
        kit = equipment_row.split(':')[-1]\
                           .replace('\n', '')

        start_date = next(row for row in data if 'started=' in row)\
                        .split('=')[-1]\
                        .replace('\n', '')

        seq_sum_file = next(row for row in data if 'sequencing_summary_file=' in row)\
                        .split('=')[-1]\
                        .replace('\n', '')
    return (cell, kit, start_date, seq_sum_file)


def _get_freq_rate(file:str) -> str:
    """Ищет в файле запись о частоте работы поры, при необходимости конвертирует fast5>pod5, возвращает строку, содержащую число Герц"""
    # если файла нет - вываливаемся с ошибкой
    if file=='no batch file found :(':
        exit_with_error(error='', error_message=file, error_code=1) # type: ignore
    
    if file.endswith('.pod5'):
        output = run_shell_cmd(cmd=f'pod5 view --include "sample_rate" {file} | uniq')
    elif file.endswith('.fast5'):
        run_shell_cmd(cmd=f'pod5 convert fast5 {file} --output /tmp/freq.pod5 --threads 4')
        output = run_shell_cmd(cmd=f'pod5 view --include "sample_rate" /tmp/freq.pod5 | uniq')
        run_shell_cmd(cmd=f'rm /tmp/freq.pod5')
    
    try:
        freq = output['stdout'].split('\n')[1] # type: ignore
    except IndexError:
        print(output['stdout']) # type: ignore
        exit()
    
    return freq
    