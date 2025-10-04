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
import re

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
                elem.clear()
                return pd.DataFrame([info])
    except Exception as e:
        error_info = {
            "zipfile": os.path.basename(file_path),
            "xml_filename": fname,
            "root_tag": None,
            "error": str(e)
        }
        return pd.DataFrame([error_info])

def get_description_element(archive, fname):
      """
      Извлекает элемент <description> из XML-файла в архиве и очищает его от пространств имен.

      Args:
          archive (zipfile.ZipFile): Объект zip-архива.
          fname (str): Имя файла внутри архива.

      Returns:
          lxml.etree._Element or None: XML-объект тега <description> без пространств имен или None, если тег не найден.
      """
      try:
          with archive.open(fname) as fileobj:
              context = etree.iterparse(fileobj, events=("end",), tag="{*}description")
              
              # Берем первый элемент с помощью next()
              event, elem = next(context)

            # Очищаем пространства имен
            # 1. Удаляем префикс из имени тега у самого элемента и всех его потомков
              for el in elem.iter():
                  el.tag = etree.QName(el).localname
                  # Удаляем namespace из атрибутов
                  for attr_name in list(el.attrib):
                    local_attr_name = etree.QName(attr_name).localname
                    if attr_name != local_attr_name:
                      el.attrib[local_attr_name] = el.attrib.pop(attr_name)
                            
              # 2. Удаляем объявления xmlns, которые теперь не используются
              etree.cleanup_namespaces(elem)

          return elem

      except StopIteration:
          # next() не нашел элемент <description>
          return None
      except Exception:
          # Любая другая ошибка (парсинг, открытие файла и т.д.)
          return None

def description_string(description_element):
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
    data = {}
    # Очищаем от символов форматирования и атрибута namespace выгружая в строку и загружая из строки но уже с парсером
    xml_str_clean = re.sub(r'<description[^>]*>', r'<description>', etree.tostring(description_element, encoding='unicode'))
    parser = etree.XMLParser(remove_blank_text=True)
    cleaned_elem = etree.fromstring(xml_str_clean, parser)  
 
    data["description"] = etree.tostring(cleaned_elem, encoding='unicode')
    
    return data

def description_taglist(description_element):
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
    data = {}
    # Итерируемся по прямым потомкам <description> (например, <title-info>, <document-info>)
    for element in description_element:
        # Имя тега без пространства имен (станет названием столбца)
        col_name = element.tag
        
        # Собираем имена дочерних тегов для каждого элемента
        child_tags = [child.tag for child in element]
        child_tags.sort()
        
        # Записываем отсортированные и объединенные имена тегов
        data[col_name] = ", ".join(child_tags)
    
    return data

def description_child_ontag(description_element, child_tag_name):
    """
    Анализирует входящее поддерво <description> и извлекает текст из указанных дочерних тегов.

    Args:
        child_tag_name (str): Имя дочернего тега внутри <description>, текст которого нужно извлечь.

    Returns:
        info_res: dict с результатом. Если найдено несколько тегов, создает нумерованные столбцы.
    """                   
    info_res = {}
    
    # Находим все теги с заданным именем на любом уровне вложенности внутри <description>
    found_tags = description_element.xpath(f".//{child_tag_name}")
    
    # Даже если тег один, добавляем цифру для унификации
    if not found_tags:
        # Если теги не найдены, все равно создаем столбец с пустым значением
        info_res[f"{child_tag_name}1"] = None
    else:
        for i, tag in enumerate(found_tags, 1):
            col_name = f"{child_tag_name}{i}"
            # itertext() итерирует по всему тексту из элемента и его потомков
            text = " ".join([s.strip() for s in tag.itertext() if s.strip()])
            info_res[col_name] = text
    
    return info_res

def description_child_ontag_all(description_element, child_tag_name):
    """
    Анализирует входящее поддерво <description> и извлекает текст из указанных дочерних тегов.

    Args:
        child_tag_name (str): Имя дочернего тега внутри <description>, текст которого нужно извлечь.

    Returns:
        info_res: dict с результатом. Если найдено несколько тегов, создает нумерованные столбцы.
    """                   
    info = description_child_ontag(description_element, child_tag_name)
    info_res = {child_tag_name: '; '.join(str(v) for v in info.values())}
    
    return info_res

def get_authors_string(description_element):
      """
      Находит всех авторов в description_element, собирает их полные имена
      и возвращает в виде одной строки, разделенной точкой с запятой.
      """
      info = {}
      # 1. Находим все элементы <author> на любом уровне вложенности
      author_elements = description_element.xpath('.//title-info/author')

      # 2. С помощью спискового выражения (list comprehension) создаем список полных имен
      full_names = [
          # 3. Для каждого автора, объединяем части имени через пробел
          ' '.join(part for part in [
              # 4. Находим текст каждого тега, убираем лишние пробелы.
              # Если тега нет, findtext вернет пустую строку.
              author.findtext('first-name', default='').strip(),
              author.findtext('middle-name', default='').strip(),
              author.findtext('last-name', default='').strip()
          ] if part) # 'if part' отфильтровывает пустые строки (если, например, нет отчества)
          for author in author_elements
      ]

      # 5. Объединяем список полных имен в одну строку через ';'
      # Дополнительно фильтруем пустые имена, если вдруг попадется тег <author> без дочерних элементов
      final_authors_string = '; '.join(name for name in full_names if name)
      ids = '; '.join(part for part in [author.findtext('id', default='').strip() for author in author_elements] if part)

      # 6. Возвращаем результат в требуемом формате
      info['author'] = final_authors_string
      info['id_author'] = ids
      return info

def catalog(description_element):
    """
    Анализирует входящее поддерво <description> и извлекает текст из указанных дочерних тегов.

    Args:
        child_tag_name (str): Имя дочернего тега внутри <description>, текст которого нужно извлечь.
des
    Returns:
        info_res: dict с результатом. Если найдено несколько тегов, создает нумерованные столбцы.
    """                   
    info_res = get_authors_string(description_element)
    info_genre = description_child_ontag_all(description_element, "title-info/genre")
    info_book_title = description_child_ontag_all(description_element, "title-info/book-title")
    info_lang = description_child_ontag_all(description_element, "title-info/lang")
    info_allstring = description_string(description_element)
    info_res.update(info_genre)
    info_res.update(info_book_title)
    info_res.update(info_lang)
    info_res.update(info_allstring)

    return info_res

def description_processor(archive, fname, file_path, func, **kwargs):
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
                    
        description_element = get_description_element(archive, fname)

        if description_element is None:
            raise ValueError("Тег <description> не найден в файле.")

        info = {
            "zipfile": os.path.basename(file_path),
            "xml_filename": fname,
        }
        
        processor_func = DESC_PROCESSORS.get(func)

        if processor_func:
            # Вызываем фукнцию обработчик тэга которая по идее всегда возвращает словарь
            info_res = processor_func(description_element, **kwargs)
            info.update(info_res)
            return pd.DataFrame([info])
        else:
            # Если нет, сообщаем об ошибке
            raise ValueError("Функция обработчик не найдена")       
         
    except Exception as e:
        error_info = {
            "zipfile": os.path.basename(file_path),
            "xml_filename": fname,
            "error": str(e)
        }
        return pd.DataFrame([error_info])

DESC_PROCESSORS = {
    "description_child_ontag": description_child_ontag,
    "description_taglist": description_taglist,
    "description_string": description_string,
    "description_child_ontag_all": description_child_ontag_all,
    "get_authors_string": get_authors_string,
    "catalog": catalog
  }