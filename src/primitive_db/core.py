def create_table(metadata: dict, table_name: str, columns: list) -> dict:
    """
    Создает новую таблицу в метаданных.

    Принимает:
        metadata (dict): текущий словарь метаданных
        table_name (str): название новой таблицы
        columns (list): список строк, описывающих столбцы таблицы 
    
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
    if not all(isinstance(col, str) for col in columns):
        raise TypeError('Описание столбца должно иметь строковый тип.')

    ALLOWED_TYPES = {'str', 'int', 'bool'}

    processed_columns = {}
    
    columns_dict = {col.split(':')[0] : col.split(':')[1] for col in columns}
    if 'id' not in columns_dict.keys():
        id_column = ['ID', 'int']
        processed_columns[id_column[0]] = id_column[1]

    for name, type in columns_dict.items():
        if type not in ALLOWED_TYPES:
            raise ValueError((f"Некорректный тип данных '{type}'" 
                              f" для столбца '{name}'."
                              f"Разрешены только: {', '.join(ALLOWED_TYPES)}"))
        if 'id' in name:
            name = name.upper()
        processed_columns[name] = type

    new_table = {
        "columns": processed_columns
    }
    metadata[table_name] = new_table

    column_display = ', '.join([f'{col_name}:{col_type}' for col_name, col_type in processed_columns.items()]) #noqa: E501
    print(f"Таблица с именем '{table_name}' успешно создана со столбцами: {column_display}.") #noqa: E501
    return metadata 


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
    print(f"Таблица с именем '{table_name}' успешно удалена.")
    return metadata


def list_tables(metadata: dict) -> list:
    """
    Возвращает список имен всех таблиц в метаданных.
    """
    return list(metadata.keys())