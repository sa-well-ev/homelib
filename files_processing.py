import os
import zipfile
import pandas as pd
import numpy as np
import sqlite3
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
    """Удаляет файлы из zip-архивов на основе списка из базы данных.

    Функция получает из базы данных список файлов, которые необходимо удалить.
    Она перебирает zip-архивы, указанные в списке, и создает их временные
    копии, не включая файлы, предназначенные для удаления. Затем исходные
    архивы заменяются их очищенными версиями.

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
    df = get_table_cols(tbl_name=tbl_name, tbl_cols = tbl_cols)
    folder = os.path.dirname(file_name)
    for zipname, files in df.groupby('zipfile')['xml_filename']:
        zipname = os.path.join(folder,zipname)
        files_to_delete = set(files)
        temp_zip = zipname + '.tmp'
        with zipfile.ZipFile(zipname, 'r') as zin, zipfile.ZipFile(temp_zip, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zout:
            for item in zin.infolist():
                if item.filename not in files_to_delete:
                    zout.writestr(item, zin.read(item.filename))
        os.replace(temp_zip, zipname)
        print(f'Файл {zipname} очищен на {len(files_to_delete)} файлов из {len(zin.infolist())}')
    return