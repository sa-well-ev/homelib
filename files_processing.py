import os
import zipfile
import pandas as pd
from xml_processor import description_processor

def process_zipfile(file_path, func, **kwargs):
    """
    Перебирает все файлы в zip-архиве и запускает обработку каждого файла выбранной функцией

    Args:
        file_path (str): Путь к zip-архиву.
        processing_func (function): Функция для обработки каждого файла в архиве.

    Returns:
        pd.DataFrame: DataFrame с информацией о каждом FB2-файле в архиве.
    """
    all_result = pd.DataFrame()
    with zipfile.ZipFile(file_path) as archive:
        for fname in archive.namelist():
            if fname.endswith(".fb2") and not fname.endswith("/"):
                df = description_processor(archive, fname, file_path, func, **kwargs)
                all_result = pd.concat([all_result, df], ignore_index=True)
    return all_result


def process_zipfolder(file_name, func, **kwargs):
    """
    Перебирает все zip-архивы в директории файла и запускает обработку каждого архива

    Args:
        file_name (str): Путь к файлу, который используется для определения рабочей директории.
        processing_func (function): Функция для обработки каждого файла в архиве.

    Returns:
        pd.DataFrame: DataFrame с информацией обо всех FB2-файлах из всех найденных zip-архивов.
    """
    folder = os.path.dirname(file_name)
    zip_files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.endswith(".zip") and os.path.isfile(os.path.join(folder, f))
    ]
    all_result = pd.DataFrame()
    for zip_path in zip_files:
        df = process_zipfile(zip_path, func, **kwargs)
        all_result = pd.concat([all_result, df], ignore_index=True)
    return all_result
