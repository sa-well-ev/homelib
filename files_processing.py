import os
import zipfile
import pandas as pd
import numpy as np
import sqlite3
import subprocess
import tempfile
from xml_processor import description_processor
from desc_analys import get_table_cols

def process_zipfile(file_path, func, bd_insert, table_insert, **kwargs):
    """
    Обрабатывает FB2-файлы в zip-архиве.

    Перебирает все файлы в zip-архиве, извлекает FB2-файлы и обрабатывает их
    с помощью указанной функции. Результаты могут быть сохранены в базу данных
    или возвращены в виде DataFrame.

    Args:
        file_path (str): Путь к zip-архиву.
        func (function): Функция для обработки каждого FB2-файла.
        bd_insert (bool): Если True, результаты вставляются в базу данных.
        table_insert (str): Имя таблицы для вставки данных.
        **kwargs: Дополнительные аргументы для функции обработки.

    Returns:
        pd.DataFrame or None: DataFrame с результатами обработки, если `bd_insert`
                              равно False, в противном случае None.
    """
    all_result = pd.DataFrame()
    with zipfile.ZipFile(file_path) as archive:
        for fname in archive.namelist():
            if fname.endswith(".fb2") and not fname.endswith("/"):
                df = description_processor(archive, fname, file_path, func, **kwargs)
                with pd.option_context('future.no_silent_downcasting', True):
                     df.replace('', np.nan, inplace=True)                
                all_result = pd.concat([all_result, df], ignore_index=True)
    if bd_insert:
        conn = sqlite3.connect('./data/homelib.lite')
        all_result.to_sql(table_insert, conn, if_exists='append', index=False)
        conn.close()
        return
    else:
        return all_result

def process_zipfolder(file_name, func, bd_insert=False, table_insert="lib_current", **kwargs):
    """
    Обрабатывает все zip-архивы в указанной директории.

    Находит все zip-архивы в директории, где находится `file_name`,
    и для каждого архива запускает `process_zipfile`.

    Args:
        file_name (str): Путь к файлу, используемый для определения рабочей директории.
        func (function): Функция для обработки каждого FB2-файла.
        bd_insert (bool, optional): Если True, результаты вставляются в базу данных.
                                    Defaults to False.
        table_insert (str, optional): Имя таблицы для вставки данных.
                                      Defaults to "lib_current".
        **kwargs: Дополнительные аргументы для функции обработки.

    Returns:
        pd.DataFrame or None: DataFrame с результатами, если `bd_insert` равно False,
                              в противном случае None.
    """
    folder = os.path.dirname(file_name)
    zip_files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.endswith(".zip") and os.path.isfile(os.path.join(folder, f))
    ]
    if bd_insert:
        for zip_path in zip_files:
            df = process_zipfile(zip_path, func, bd_insert, table_insert, **kwargs)
            print(f'Файл {zip_path} записан в {table_insert}')            
        return
    else:
        all_result = pd.DataFrame()
        for zip_path in zip_files:
            df = process_zipfile(zip_path, func, bd_insert, table_insert, **kwargs)
            all_result = pd.concat([all_result, df], ignore_index=True)
        return all_result
    
def clear_zipfolder(file_name='./lib', tbl_name='lib_delete', tbl_cols='zipfile, xml_filename'):
    """Удаляет файлы из zip-архивов на основе списка из базы данных с использованием 7z.

    Функция получает из базы данных список файлов, которые необходимо удалить.
    Она перебирает zip-архивы, указанные в списке, и вызывает утилиту 7z
    для удаления перечисленных файлов непосредственно из архива.

    Args:
        file_name (str, optional): Путь к файлу или директории, используемый для
                                   определения рабочей папки с архивами.
                                   Defaults to './lib'.
        tbl_name (str, optional): Имя таблицы в базе данных, содержащей
                                  информацию об удаляемых файлах.
                                  Defaults to 'lib_delete'.
        tbl_cols (str, optional): Имена столбцов для извлечения из таблицы
                                  (имя zip-архива и имя файла внутри архива),
                                  перечисленные через запятую.
                                  Defaults to 'zipfile, xml_filename'.

    Returns:
        None
    """
    df = get_table_cols(tbl_name=tbl_name, tbl_cols=tbl_cols)
    folder = os.path.dirname(file_name)
    for zipname, files in df.groupby('zipfile')['xml_filename']:
        zip_path = os.path.join(folder, zipname)
        files_to_delete = list(files)

        if not os.path.exists(zip_path):
            print(f"Архив {zip_path} не найден, пропуск.")
            continue

        if not files_to_delete:
            print(f"Для архива {zip_path} нет файлов для удаления.")
            continue

        list_filename = ''
        try:
            # Создаем временный файл со списком файлов для удаления,
            # чтобы избежать проблем с ограничением длины командной строки.
            with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', suffix=".txt") as listfile:
                listfile.write('\n'.join(files_to_delete))
                list_filename = listfile.name
            
            command = ['7z', 'd', zip_path, f'@{list_filename}', '-y']
            
            subprocess.run(command, check=True, capture_output=True, text=True, encoding='cp866')
            print(f"Из архива {zip_path} удалено {len(files_to_delete)} файлов.")

        except FileNotFoundError:
            print("Ошибка: '7z' не найден. Убедитесь, что он установлен и прописан в PATH.")
            break
        except subprocess.CalledProcessError as e:
            print(f"Ошибка при удалении файлов из {zip_path}:")
            print(e.stderr)
        except Exception as e:
            print(f"Произошла непредвиденная ошибка при обработке {zip_path}: {e}")
        finally:
            # Удаляем временный файл
            if list_filename and os.path.exists(list_filename):
                os.remove(list_filename)
            
    return

import shutil

def repack_zipfolder(file_name, dest_folder="errors", tbl_name='lib_errors', tbl_cols='zipfile, xml_filename'):
    """Создает копии zip-архивов, включая только указанные файлы, с использованием 7z.

    Функция получает из базы данных список файлов для включения в новые архивы.
    Она использует утилиту 7z для извлечения нужных файлов из исходного
    архива во временную папку, а затем создает новый архив в `dest_folder`
    из этих файлов, сохраняя структуру каталогов.

    Args:
        file_name (str): Путь к файлу или директории для определения
                         рабочей папки с исходными архивами.
        dest_folder (str): Путь к папке для сохранения новых архивов.
        tbl_name (str, optional): Имя таблицы в базе данных, содержащей
                                  информацию о включаемых файлах.
                                  Defaults to 'lib_errors'.
        tbl_cols (str, optional): Имена столбцов для извлечения из таблицы
                                  (имя zip-архива и имя файла внутри архива),
                                  перечисленные через запятую.
                                  Defaults to 'zipfile, xml_filename'.

    Returns:
        None
    """
    try:
        df = get_table_cols(tbl_name=tbl_name, tbl_cols=tbl_cols)
    except Exception as e:
        print(f"Ошибка при получении данных из таблицы {tbl_name}: {e}")
        return

    os.makedirs(dest_folder, exist_ok=True)
    source_folder = os.path.dirname(file_name)

    for zipname, files in df.groupby('zipfile')['xml_filename']:
        source_zip_path = os.path.join(source_folder, zipname)
        dest_zip_path = os.path.join(dest_folder, zipname)

        if not os.path.isfile(source_zip_path):
            print(f'Исходный архив {source_zip_path} не найден, пропуск.')
            continue

        files_to_include = list(files)
        if not files_to_include:
            print(f"Для архива {zipname} нет файлов для включения.")
            continue

        temp_dir = None
        list_filename = ''
        try:
            temp_dir = tempfile.mkdtemp()

            with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', suffix=".txt") as listfile:
                listfile.write('\n'.join(files_to_include))
                list_filename = listfile.name

            # Извлекаем нужные файлы, сохраняя структуру каталогов
            extract_command = ['7z', 'x', source_zip_path, f'-o{temp_dir}', f'@{list_filename}', '-y']
            subprocess.run(extract_command, check=True, capture_output=True, text=True, encoding='cp866')

            # Считаем, сколько файлов было фактически извлечено
            extracted_count = 0
            for _, _, f_list in os.walk(temp_dir):
                extracted_count += len(f_list)

            if extracted_count == 0:
                print(f'В {source_zip_path} не найдено файлов из списка. Архив {dest_zip_path} не создан.')
                continue

            # Создаем новый архив из извлеченных файлов
            abs_dest_zip_path = os.path.abspath(dest_zip_path)
            add_command = ['7z', 'a', abs_dest_zip_path, '*', '-y']
            subprocess.run(add_command, check=True, capture_output=True, text=True, encoding='cp866', cwd=temp_dir)
            
            print(f'Создан архив {dest_zip_path} с {extracted_count} файлами.')

        except FileNotFoundError:
            print("Ошибка: '7z' не найден. Убедитесь, что он установлен и прописан в PATH.")
            break
        except subprocess.CalledProcessError as e:
            if "No files to process" in e.stdout or "No files to process" in e.stderr:
                 print(f'В {source_zip_path} не найдено файлов из списка. Архив {dest_zip_path} не создан.')
            else:
                print(f"Ошибка при обработке архива {zipname}:")
                print(f"Команда: {' '.join(e.args)}")
                print(f"Stderr: {e.stderr}")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка при обработке {zipname}: {e}")
        finally:
            if list_filename and os.path.exists(list_filename):
                os.remove(list_filename)
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
    return
