import json

from prettytable import PrettyTable

from src.decorators import confirm_action, create_cacher, handle_db_errors, log_time
from src.primitive_db.constants import ALLOWED_TYPES, TYPE_MAPPING
from src.primitive_db.parser import define_value_type


@handle_db_errors
def create_table(metadata: dict, table_name: str, columns: list) -> dict:
    """
    Создает новую таблицу в метаданных.

    Принимает:
        metadata (dict): текущий словарь метаданных
        table_name (str): название новой таблицы
        columns (dict): словарь, описывающий столбцы таблицы 
    
    Возвращает:
        metadata (dict): обновленный словарь метаданных с случае успеха.
    
    Вызывает исключение ValueError, если:
    - Таблица с введенным названием уже существует.
    - Обнаружен некорректный тип данных для столбца.
    
    Вызывает исключение TypeError, если:
    - Обнаружен некорректный тип данных описания столбца.
    """

    if table_name in metadata:
        raise ValueError(f'Таблица с именем "{table_name}" уже существует.')
    if not isinstance(columns, dict):
        raise TypeError('Не удалось создать столбцы.')
    if not all(isinstance(col, str) for col in columns):
        raise TypeError('Описание столбца должно иметь строковый тип.')

    processed_columns = {}
    
    if 'id' not in list(columns.keys()):
        id_column = ['ID', 'int']
        processed_columns[id_column[0]] = id_column[1]

    for col_name, col_type in columns.items():
        if col_type not in ALLOWED_TYPES:
            raise ValueError((f"Некорректный тип данных '{col_type}'" 
                              f" для столбца '{col_name}'.\n"
                              f"Разрешены только: {', '.join(ALLOWED_TYPES)}\n"
                               "Попробуйте снова."))
        if 'id' in col_name:
            col_name = col_name.upper()
        processed_columns[col_name] = col_type

    new_table = {
        "columns": processed_columns
    }
    metadata[table_name] = new_table

    column_display = ', '.join([f'{col_name}:{col_type}' for col_name, col_type in processed_columns.items()]) #noqa: E501
    print(f"Таблица с именем '{table_name}' успешно создана со столбцами: {column_display}.") #noqa: E501
    return metadata 

@confirm_action("удаление таблицы")
@handle_db_errors
def drop_table(metadata: dict, table_name: str) -> dict:
    """
    Удаляет таблицу из метаданных.

    Принимает:
        metadata (dict): текущие метаданные
        table_name (str): имя таблицы на удаление
    
    Возвращает:
        metadata (dict): обновленный словарь метаданных
        
    Вызывает ValueError, если:
    - таблицы с указанным именем не существует
    """
    if table_name not in metadata:
        raise ValueError(f"Таблицы '{table_name}' не существует.")
    del metadata[table_name]
    return metadata

@handle_db_errors
def list_tables(metadata: dict) -> list:
    """
    Возвращает список имен всех таблиц в метаданных.
    """
    return list(metadata.keys())

@handle_db_errors
@log_time
def insert(metadata: dict, table_name: str, values: list) -> dict:
    """
    Добавляет записи в таблицу.

    Вызывает ValueError, если:
    - Таблица с указанным названием не найдена.
    - Количество введенных значений не совпадает с количеством столбцов в таблице.
    - Введен неверный тип данных для столбца.
    """
    if table_name not in metadata:
        raise ValueError(f'Таблицы {table_name} нет.')
    column_names = list(metadata[table_name]['columns'].keys())
    if len(values) != len(column_names) - 1:
        raise ValueError((f'Количество введенных значений не совпадает '
                          f'с количеством столбцов в таблице {table_name}.'))
    
    column_types = list(metadata[table_name]['columns'].values())
    new_line = {}

    if 'data' not in metadata[table_name]:
        new_id = 1
    else:
        data = metadata[table_name]['data']
        new_id = len(data) + 1
    new_line[column_names[0]] = new_id
    
    for i in range(len(values)):
        expected_type = TYPE_MAPPING[column_types[i + 1]]
        value = define_value_type(values[i])
        column_name = column_names[i + 1]
        if not isinstance(value, expected_type):
            raise ValueError((f'Неверный тип данных для столбца {column_name}.\n'
                             f'Ожидался: {expected_type}\n'
                             f'Получен: {type(value)}.\n'
                              'Попробуйте снова.'))
        new_line[column_name] = value
    if 'data' not in metadata[table_name]:
        metadata[table_name]['data'] = []
    metadata[table_name]['data'].append(new_line)
    print('Запись успешно добавлена.')

    return metadata

@handle_db_errors
def create_row_filter_function(column, value, all_column_names):
    column_index = all_column_names.index(column)
    def row_filter(row):
        if column_index < len(row):
            return row[column_index] == value 
        else:
            return False
    return row_filter


query_cacher = create_cacher()
@handle_db_errors
@log_time
def select(table_data: dict, where_clause=None) -> PrettyTable:
    """
    Если не передано условие фильтрации, выводит на экран всю таблицу.
    Если задано условие фильтрации, выводит только нужную строку.

    Использует механизм кэширования: 
    - если запрос вызывался ранее, он будет возвращен из кэша.
    - если запрос не вызывался, он будет сформирован и сохранен в кэш.

    Вызывает ValueError, если:
    - Передан неверный тип данных для столбца.
    - Таблица с указанным названием не найдена.
    """
    key_data_part = json.dumps(table_data, sort_keys=True)
    key_where_part = json.dumps(where_clause, sort_keys=True) if where_clause else 'NONE' #noqa: E501
    cache_key = (key_data_part, key_where_part)

    def execute_query():
        if not table_data:
            raise ValueError('Такой таблицы нет.')
        column_names = list(table_data['columns'].keys())
        data = table_data.get('data')
        if data:
            rows = [list(row.values()) for row in table_data['data']]
        else:
            rows = []
        table = PrettyTable()
        table.field_names = column_names
        table.add_rows(rows)
        if not where_clause:
            return table
        else:
            column_name = list(where_clause.keys())[0]
            column_type = table_data['columns'][column_name]
            expected_type = TYPE_MAPPING[column_type]
            value = where_clause[column_name]
            if not isinstance(value, expected_type):
                raise ValueError((f'Неверный тип данных для столбца "{column_name}"\n'
                                        f'Ожидался: {expected_type}\n'
                                        f'Получен: {type(value)}'))
            filter_function = create_row_filter_function(column_name, value, column_names) #noqa: E501
            return table.get_string(row_filter=filter_function)
    if cache_key:
        result = query_cacher(cache_key, execute_query)
    else:
        result = execute_query()
    return result


def update(table_data: dict, set_clause: dict, where_clause: dict) -> dict:
    """
    Обновляет значение в таблице по заданному условию.
    Возвращает обновленную таблицу.

    Вызывает ValueError, если 
    - Указанной таблицы нет в базе данных.
    - Введен неверный тип данных для столбца.
    """
    if not table_data:
        raise ValueError('Такой таблицы нет.')
    
    set_column = list(set_clause.keys())[0]
    set_value = set_clause[set_column]
    where_column = list(where_clause.keys())[0]
    where_value = where_clause[where_column]

    set_column_type = table_data['columns'][set_column] 
    expected_type = TYPE_MAPPING[set_column_type]

    rows = table_data['data']
    updated_rows = []
    for row in rows:
        if row[where_column] == where_value:
            if not isinstance(set_value, expected_type):
                raise ValueError((f'Неверный тип данных для столбца "{set_column}"'
                                  f'Ожидался: "{set_column_type}"'
                                  f'Получен: "{type(set_value)}"'))
            row[set_column] = set_value
            updated_rows.append(row)
    if updated_rows:
        if len(updated_rows) == 1:
            print('Обновлена 1 запись')
        else: 
            print(f'Обновлено {len(updated_rows)} записей.')
    else:
        print(f'В таблице нет соответствий условию "{where_column} = {where_value}"')
    table_data['data'] = rows

    return table_data

@confirm_action("удаление записи")
@handle_db_errors
def delete(table_data: dict, where_clause: dict) -> dict:
    """
    Функция находит записи по условию и удаляет их.
    Сдвигает индексы.
    Возвращает измененные данные.

    Вызывает ValueError, если 
    - Указанной таблицы нет в базе данных.
    """
    if not table_data:
        raise ValueError('Такой таблицы нет.')
    column = list(where_clause.keys())[0]
    value = where_clause[column]
    rows = table_data['data']
    for row in rows:
        if row[column] == value:
            row_index = rows.index(row)
            rows.remove(row)
            for i in range(row_index, len(rows)):
                rows[i]['ID'] = i + 1
    table_data['data'] = rows 

    return table_data

@handle_db_errors
def info(table_data: dict, table_name: str) -> None:
    """
    Выводит информацио о таблице: название, столбцы, количество записей.

    Вызывает ValueError, если 
    - Указанной таблицы нет в базе данных.
    """
    if not table_data:
        raise ValueError('Такой таблицы нет.')
    columns = [f'{str(col)}:{str(dtype)}' for col, dtype in table_data['columns'].items()] #noqa: E501
    columns = ', '.join(columns)
    if 'data' not in list(table_data.keys()):
        n_rows = 0
    else:
        n_rows = len(table_data['data'])
    print(f'Таблица: {table_name}')
    print(f'Столбцы: {columns}')
    print(f'Количество записей: {n_rows}')