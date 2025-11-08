import json
import os

from prettytable import PrettyTable

DB_METADATA_FILE = 'db_meta.json'

def load_metadata(filepath: str) -> dict:
    """
    Загружает метаданные из JSON-файла.
    Если файл не найден, возвращает пустой словарь.
    Обрабатывает FileNotFoundError.
    """
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Файл '{filepath}' не найден.")
        return {}
    except Exception as e:
        print(f"Непредвиденная ошибка при загрузке метаданных из '{filepath}': {e}")
        return {}

def save_metadata(filepath: str, data: dict):
    """
    Сохраняет переданные метаданные (словарь) в JSON-файл.
    Автоматически создает директорию, если ее не существует.
    """
    dirname = os.path.dirname(filepath)
    if dirname and not os.path.exists(filepath):
        os.makedirs(filepath)
    try:
        with open(filepath, mode='w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"Ошибка при сохранении метаданных в '{filepath}': {e}")

def load_table_data(filepath: str) -> dict:
    """
    Загружает данные таблицы из JSON-файла.
    Если файл не найден, возвращает пустой словарь.
    Обрабатывает FileNotFoundError.
    """
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            table = json.load(f)
        return table 
    except FileNotFoundError:
        print(f'Файл {filepath} не найден.')
        return {}
    except Exception as e:
        print(f'Возникла непредвиденная ошибка при чтении данных из {filepath}: {e}')
        return {}


def save_table_data(filepath: str, table: dict):
    """
    Сохраняет данные о таблице в JSON-файл.
    Автоматически создает директорию, если ее не существует.
    """
    dirname = os.path.dirname(filepath)
    if dirname and not os.path.exists(filepath):
        os.makedirs(filepath)
    try:
        with open(filepath, mode='w', encoding='utf-8') as f:
            json.dump(table, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f'Возникла непредвиденная ошибка при сохранении данных в {filepath}: {e}')

def show_table(table_data: dict) -> PrettyTable:
    columns = list(table_data['columns'].keys())
    rows = [list(row.values()) for row in table_data['data']]
    table = PrettyTable()
    table.field_names = columns 
    table.add_rows(rows)
    return table