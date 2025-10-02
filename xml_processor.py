"""
Пакет xml_processor.

Содержит набор функций для обрабоки отдельного файла формата FB2

Все входящие в пакет функции должны быть унифицированны и поддерживать:
Args:
        archive (zipfile.ZipFile): Объект zip-архива.
        fname (str): Имя файла внутри архива.
        file_path (str): Путь к zip-архиву.

Returns:
        pd.DataFrame: DataFrame с результатом обработки

"""

import os
import pandas as pd
from lxml import etree

def namespaces(archive, fname, file_path):
    """
    Анализирует XML-файл (FB2) внутри zip-архива для извлечения корневого тега и пространств имен.

    Args:
        archive (zipfile.ZipFile): Объект zip-архива.
        fname (str): Имя файла внутри архива.
        file_path (str): Путь к zip-архиву.

    Returns:
        pd.DataFrame: DataFrame с информацией о файле. В случае ошибки возвращает DataFrame с информацией об ошибке.
    """
    try:
        with archive.open(fname) as fileobj:
            for event, elem in etree.iterparse(fileobj, events=("start",)):
                nsmap = dict(elem.nsmap)
                root_tag = elem.tag
                info = {
                    "zipfile": os.path.basename(file_path),
                    "xml_filename": fname,
                    "root_tag": root_tag
                }
                for k, v in nsmap.items():
                    info[f'ns_{k if k else "default"}'] = v
                return pd.DataFrame([info])
    except Exception as e:
        error_info = {
            "zipfile": os.path.basename(file_path),
            "xml_filename": fname,
            "root_tag": None,
            "error": str(e)
        }
        return pd.DataFrame([error_info])


def tag_description(archive, fname, file_path):
    """
    Анализирует XML-файл (FB2) и извлекает информацию из тега <description>.

    Находит все дочерние элементы тега <description>, использует их имена в качестве столбцов DataFrame.
    Значениями служат отсортированные по алфавиту и перечисленные через запятую имена
    внутренних тегов каждого дочернего элемента.

    Args:
        archive (zipfile.ZipFile): Объект zip-архива.
        fname (str): Имя файла внутри архива.
        file_path (str): Путь к zip-архиву.

    Returns:
        pd.DataFrame: DataFrame с результатом обработки. В случае ошибки возвращает DataFrame с информацией об ошибке.
    """
    try:
        with archive.open(fname) as fileobj:
            # Используем etree.parse для получения полного дерева
            tree = etree.parse(fileobj)
            root = tree.getroot()
            
            # Ищем тег description, игнорируя пространство имен в поиске
            # Это проще и надежнее, если структура FB2 предсказуема
            description_element = root.find('{*}description')

            if description_element is None:
                raise ValueError("Тег <description> не найден в файле.")

            data = {}
            # Итерируемся по прямым потомкам <description> (например, <title-info>, <document-info>)
            for element in description_element:
                # Имя тега без пространства имен (станет названием столбца)
                col_name = etree.QName(element).localname
                
                # Собираем имена дочерних тегов для каждого элемента
                child_tags = [etree.QName(child).localname for child in element]
                child_tags.sort()
                
                # Записываем отсортированные и объединенные имена тегов
                data[col_name] = ", ".join(child_tags)
            
            # Добавляем базовую информацию о файле для консистентности
            info = {
                "zipfile": os.path.basename(file_path),
                "xml_filename": fname,
            }
            info.update(data)
            
            return pd.DataFrame([info])
            
    except Exception as e:
        error_info = {
            "zipfile": os.path.basename(file_path),
            "xml_filename": fname,
            "error": str(e)
        }
        return pd.DataFrame([error_info])

def tag_description_child(archive, fname, file_path, child_tag_name):
    """
    Анализирует XML-файл (FB2), находит тег <description> и извлекает текст из указанных дочерних тегов.

    Args:
        archive (zipfile.ZipFile): Объект zip-архива.
        fname (str): Имя файла внутри архива.
        file_path (str): Путь к zip-архиву.
        child_tag_name (str): Имя дочернего тега внутри <description>, текст которого нужно извлечь.

    Returns:
        pd.DataFrame: DataFrame с результатом. Если найдено несколько тегов, создает нумерованные столбцы.
    """
    try:
        with archive.open(fname) as fileobj:
            tree = etree.parse(fileobj)
            root = tree.getroot()
            
            description_element = root.find('{*}description')

            if description_element is None:
                raise ValueError("Тег <description> не найден в файле.")

            # Находим все теги с заданным именем на любом уровне вложенности внутри <description>
            found_tags = description_element.findall('.//{*}' + child_tag_name)
            
            info = {
                "zipfile": os.path.basename(file_path),
                "xml_filename": fname,
            }
            
            # Даже если тег один, добавляем цифру для унификации
            if not found_tags:
                # Если теги не найдены, все равно создаем столбец с пустым значением
                info[f"{child_tag_name}1"] = None
            else:
                for i, tag in enumerate(found_tags, 1):
                    col_name = f"{child_tag_name}{i}"
                    # itertext() итерирует по всему тексту из элемента и его потомков
                    text = " ".join(tag.itertext()).strip()
                    info[col_name] = text
            
            return pd.DataFrame([info])
            
    except Exception as e:
        error_info = {
            "zipfile": os.path.basename(file_path),
            "xml_filename": fname,
            "error": str(e)
        }
        return pd.DataFrame([error_info])
