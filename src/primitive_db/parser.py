from src.decorators import handle_db_errors


def define_value_type(value):
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    elif value.isdigit():
        return int(value)
    else:
        return str(value)

@handle_db_errors
def insert_columns_parser(columns: list) -> dict:
    """
    Парсит строку с названием и типом столбцов в словарь.
    Пример: "name:str" -> {'name': 'str'}

    Вызывает ValueError, если:
    - Описание столбца введено в неправильном формате.
    """
    for column in columns:
        if ":" not in column:
            raise ValueError('Неверный формат ' 
                        'записи описания столбца.\n'
                        'Ожидается: <имя_столбца:тип>')
    columns_dict = {column.split(':')[0] : column.split(':')[1] for column in columns}
    return columns_dict

def where_clause_parser(clause: list) -> dict:
    """
    Парсит условие фильтрации (например, ['age', '=', '28']) 
    в словарь типа {'age': 28}
    """
    column_name = clause[0]
    value = define_value_type(clause[2])

    where_clause = {
        column_name: value
    }

    return where_clause


def set_clause_parser(clause: list) -> dict:
    """
    Парсит комнаду по обновлению записи по условию
    (пример ввода: ['age', '=', '28'])
    в словарь типа {'age': 28}
    """
    column_name = clause[0]
    value = define_value_type(clause[2])

    set_clause = {
        column_name: value
    }

    return set_clause