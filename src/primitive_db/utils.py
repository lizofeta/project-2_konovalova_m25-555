import json
import os

from prettytable import PrettyTable

from src.decorators import handle_db_errors


@handle_db_errors
def load_metadata(filepath: str) -> dict:
    """
    Загружает метаданные из JSON-файла.
    """
    with open(filepath, mode='r', encoding='utf-8') as f:
        data = json.load(f)
    return data

@handle_db_errors
def save_metadata(filepath: str, data: dict):
    """
    Сохраняет переданные метаданные (словарь) в JSON-файл.
    Автоматически создает директорию, если ее не существует.
    """
    dirname = os.path.dirname(filepath)
    if dirname and not os.path.exists(filepath):
        os.makedirs(filepath)
    with open(filepath, mode='w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


@handle_db_errors
def load_table_data(filepath: str) -> dict:
    """
    Загружает данные таблицы из JSON-файла.
    """
    with open(filepath, mode='r', encoding='utf-8') as f:
        table = json.load(f)
    return table 


@handle_db_errors
def save_table_data(filepath: str, table: dict):
    """
    Сохраняет данные о таблице в JSON-файл.
    Автоматически создает директорию, если ее не существует.
    """
    dirname = os.path.dirname(filepath)
    if dirname and not os.path.exists(filepath):
        os.makedirs(filepath)
    with open(filepath, mode='w', encoding='utf-8') as f:
        json.dump(table, f, ensure_ascii=False, indent=4)

@handle_db_errors
def show_table(table_data: dict) -> PrettyTable:
    columns = list(table_data['columns'].keys())
    rows = [list(row.values()) for row in table_data['data']]
    table = PrettyTable()
    table.field_names = columns 
    table.add_rows(rows)
    return table