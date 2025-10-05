import sqlite3
import pandas as pd


def get_table_cols(tbl_name='lib_current', tbl_cols='zipfile, xml_filename'):
        """
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
        """
        conn = sqlite3.connect('./data/homelib.lite')
        df = pd.read_sql(f'SELECT {tbl_cols} FROM {tbl_name}', conn)     
        conn.close()
        return df

def get_col_unique(tbl_cols='title_info_genre', **kwargs):
        """
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
        """
        df = get_table_cols(tbl_cols = tbl_cols, **kwargs)
        return df[tbl_cols].str.split(';').explode().unique()