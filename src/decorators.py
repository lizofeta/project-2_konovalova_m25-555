import time
from functools import wraps


def handle_db_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            print(f'Файл не найден: {e}')
            return None
        except KeyError as e:
            print(f'Ошибка: таблица или столбец не найден: {e}')
            return None
        except ValueError as e:
            print(f'Ошибка валидации: {e}')
            return None
        except TypeError as e:
            print(f'Ошибка типа: {e}')
        except Exception as e:
            print(f'Произошла непредвиденная ошибка: {e}')
            return None
    return wrapper 


def confirm_action(action_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            answer = input((f'Вы действительно хотите выполнить "{action_name}"? [y/n]: ')) #noqa: E501
            if answer == 'y':
                return func(*args, **kwargs)
            else:
                print('Отмена операции удаления.')
                return None 
        return wrapper
    return decorator


def log_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        end = time.monotonic()
        print(f'Функция {func.__name__} выполнилась за {round(end - start, 3)} секунд.')
        return result
    return wrapper 

def create_cacher():
    """
    Создает и возвращает функцию-кэшер.
    """
    cache = {}
    def cache_result(key, value_func):
        if key in cache:
            return cache[key]
        else:
            value = value_func()
            cache[key] = value
            return value
    return cache_result 
