# HomeLib

Инструменты для анализа электронных книг в формате FB2.

## Возможности

- Анализ метаданных книг в ZIP-архивах
- Сбор информации о пространствах имён XML
- Поддержка выбора файлов через графический интерфейс

## Установка

```bash
pip install -r requirements.txt
```

## Структура проекта

### `desc_analys.py`

- **`get_table_cols(tbl_name='lib_current', tbl_cols='zipfile, xml_filename')`**
  ```
  Извлекает указанные столбцы из таблицы базы данных.

  Подключается к базе данных SQLite, выполняет запрос SELECT для получения
  указанных столбцов (`tbl_cols`) из указанной таблицы (`tbl_name`)
  и возвращает результат в виде pandas DataFrame.

  Args:
      tbl_name (str, optional): Имя таблицы для запроса.
                                Defaults to 'lib_current'.
      tbl_cols (str, optional): Строка с именами столбцов для извлечения,
                                перечисленными через запятую.
                                Defaults to 'zipfile, xml_filename'.

  Returns:
      pd.DataFrame: DataFrame с извлеченными данными.
  ```

- **`get_col_unique(tbl_cols='title_info_genre', **kwargs)`**
  ```
  Получает уникальные значения из столбца, содержащего строки с разделителями.

  Использует `get_table_cols` для извлечения одного столбца. Затем обрабатывает
  каждую строку: разделяет ее по символу точки с запятой (';'), преобразует
  полученный список в отдельные строки (explode) и возвращает массив
  уникальных значений.

  Args:
      tbl_cols (str, optional): Имя столбца для анализа.
                                Defaults to 'title_info_genre'.
      **kwargs: Дополнительные аргументы, передаваемые в `get_table_cols`
                (например, `tbl_name`).

  Returns:
      numpy.ndarray: Массив уникальных значений из указанного столбца.
  ```

### `files_processing.py`

- **`process_zipfile(file_path, func, bd_insert, table_insert, **kwargs)`**
  ```
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
  ```

- **`process_zipfolder(file_name, func, bd_insert=False, table_insert="lib_current", **kwargs)`**
  ```
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
  ```

- **`clear_zipfolder(file_name='./lib', tbl_name='lib_delete', tbl_cols='zipfile, xml_filename')`**
  ```
  Удаляет файлы из zip-архивов на основе списка из базы данных.

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
  ```

### `main.py`

- **`select_zip_file(initialdir='./lib')`**
  ```
  Открывает диалог выбора zip-файла и возвращает путь к выбранному файлу или None.

  Args:
      initialdir (str, optinal): Директория в котором открывается окно (default='./lib')        

  Returns:
      str: Полный пусть к выбранному файлу и None если файл не выбран.
  ```

### `xml_processor.py`

**Описание модуля:**
```
Пакет xml_processor.

Содержит набор функций для обрабоки отдельного файла формата FB2

Все входящие в пакет функции должны быть унифицированны и поддерживать:
Args:
        archive (zipfile.ZipFile): Объект zip-архива.
        fname (str): Имя файла внутри архива.
        file_path (str): Путь к zip-архиву.

Returns:
        pd.DataFrame: DataFrame с результатом обработки
```

- **`namespaces(archive, fname, file_path)`**
  ```
  Анализирует XML-файл (FB2) внутри zip-архива для извлечения корневого тега и пространств имен.

  Args:
      archive (zipfile.ZipFile): Объект zip-архива.
      fname (str): Имя файла внутри архива.
      file_path (str): Путь к zip-архиву.

  Returns:
      pd.DataFrame: DataFrame с информацией о файле. В случае ошибки возвращает DataFrame с информацией об ошибке.
  ```

- **`get_description_element(archive, fname)`**
  ```
  Извлекает элемент <description> из XML-файла в архиве.
  Рекурсивно очищает извлеченный элемент и всех его потомков от пространств имен в тегах и атрибутах.

  Args:
      archive (zipfile.ZipFile): Объект zip-архива.
      fname (str): Имя файла внутри архива.

  Returns:
      lxml.etree._Element or None: XML-объект тега <description> без пространств имен или None, если тег не найден.
  ```

- **`description_string(description_element)`**
  ```
  Преобразует XML-элемент в компактную строку без форматирования.

  Args:
      description_element (lxml.etree._Element): XML-элемент для обработки.

  Returns:
      dict: Словарь с одним ключом 'description', значение которого - компактная XML-строка.
  ```

- **`description_taglist(description_element)`**
  ```
  Анализирует прямых потомков элемента <description>.

  Создает словарь, где ключи - это имена тегов прямых потомков,
  а значения - отсортированный, через запятую, список тегов их собственных дочерних элементов.

  Args:
      description_element (lxml.etree._Element): XML-элемент <description>.

  Returns:
      dict: Словарь с проанализированной структурой тегов.
  ```

- **`description_child_ontag(description_element, child_tag_name)`**
  ```
  Находит все теги с именем `child_tag_name` внутри `description_element` и извлекает их текстовое содержимое.

  Args:
      description_element (lxml.etree._Element): XML-элемент для поиска.
      child_tag_name (str): Имя искомого тега.

  Returns:
      dict: Словарь, где ключи - это `child_tag_name` с добавлением номера (e.g., 'genre1'),
            а значения - объединенный текст из каждого найденного тега.
  ```

- **`description_child_ontag_all(description_element, child_tag_name)`**
  ```
  Находит все теги с именем `child_tag_name`, извлекает их текст и объединяет в одну строку.

  Использует `description_child_ontag` для поиска, а затем соединяет все найденные
  текстовые значения в одну строку через точку с запятой.

  Args:
      description_element (lxml.etree._Element): XML-элемент для поиска.
      child_tag_name (str): Имя искомого тега.

  Returns:
      dict: Словарь с одним ключом `child_tag_name` и объединенной строкой в качестве значения.
  ```

- **`get_authors_string(description_element)`**
  ```
  Извлекает полные имена и ID авторов из тега <title-info>.

  Собирает полные имена (first-name, middle-name, last-name) и ID всех авторов,
  объединяет их в две строки через точку с запятой.

  Args:
      description_element (lxml.etree._Element): XML-элемент <description>.

  Returns:
      dict: Словарь с ключами 'author' и 'id_author'.
  ```

- **`catalog(description_element)`**
  ```
  Собирает полный каталог информации из элемента <description>.

  Вызывает несколько других функций-обработчиков для извлечения информации
  об авторах, жанре, названии, языке и полной строки description,
  и объединяет все в один словарь.

  Args:
      description_element (lxml.etree._Element): XML-элемент <description>.

  Returns:
      dict: Сводный словарь с каталогом данных книги.
  ```

- **`description_processor(archive, fname, file_path, func, **kwargs)`**
  ```
  Универсальный обработчик для извлечения данных из элемента <description>.

  Находит <description> в файле, а затем вызывает указанную в `func`
  функцию-обработчик из словаря `DESC_PROCESSORS`, передавая ей
  извлеченный элемент и дополнительные аргументы `kwargs`.

  Args:
      archive (zipfile.ZipFile): Объект zip-архива.
      fname (str): Имя файла внутри архива.
      file_path (str): Путь к zip-архиву.
      func (str): Имя функции-обработчика для вызова.
      **kwargs: Дополнительные именованные аргументы для функции-обработчика.

  Returns:
      pd.DataFrame: DataFrame с результатом обработки.
  ```