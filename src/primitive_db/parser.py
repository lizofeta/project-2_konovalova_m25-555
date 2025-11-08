def define_value_type(value):
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    elif value.isdigit():
        return int(value)
    else:
        return str(value)

TYPE_MAPPING = {
    'str': str, 
    'int': int,
    'bool': bool 
}

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