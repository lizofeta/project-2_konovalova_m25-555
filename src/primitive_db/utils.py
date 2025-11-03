import os
import json

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
    except json.JSONDecodeError:
        print((f"Ошибка чтения фалйа '{filepath}': файл поврежден или"
               " содержит невалидный JSON"))
        return {}
    except Exception as e:
        print(f"Непредвиденная ошибка при загрузке метаданных из '{filepath}': {e}")
        return {}

def save_metadata(filepath: str, data: dict):
    """
    Сохраняет переданные данные (словарь) в JSON-файл.
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