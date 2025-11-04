import shlex

import prompt

from src.primitive_db.core import create_table, drop_table, list_tables
from src.primitive_db.utils import DB_METADATA_FILE, load_metadata, save_metadata


def help():
     print('\n*** Процесс работы с таблицей ***')
     print('Функции:')
     print('<command> create_table <имя_таблицы> <столбец1:тип> <столбец1:тип> .. - создать таблицу\n' #noqa: E501
           '<command> list_tables - показать список всех таблиц\n'
           '<command> drop_table <имя_таблицы> - удалить таблицу\n'
           '<command> exit - выйти из программы\n' 
           '<command> help - справочная информация')

def run():
    print('\nДобро пожаловать в примитивную базу данных!\n\n')
    print('***')
    print('Для получения списка команд введите help')

    while True:
       try:
           db_meta = load_metadata('db_meta.json')
           raw_command = prompt.string('\nВведите команду: ').lower().strip()
           command_parts = shlex.split(raw_command)
           command = command_parts[0]
           args = command_parts[1:]
              
           match command:
                  case 'help':
                     help()
                  case 'exit':
                     print('Выполняю выход из програмы. До свидания!')
                     break 
                  case 'create_table':
                     if len(args) < 2:
                         print('Ошибка: передано недостаточное количество аргументов.')
                         print('Ожидается минимум 2: <имя_таблицы> <имя_столбца:тип>')
                         continue
                     table_name = args[0]
                     raw_columns = args[1:]
                     columns_to_add = []
                     for col in raw_columns:
                         if ':' not in col:
                            print('Неверный формат записи описания столбца.')
                            print('Ожидается: <имя_столбца:тип>')
                            continue
                         columns_to_add.append(col)
                     db_meta = create_table(db_meta, table_name, columns_to_add)
                     save_metadata(DB_METADATA_FILE, db_meta)
                  case 'drop_table':
                     if len(args) != 1:
                         print('Передано неверное число аргументов.')
                         print('Ожидается 1: <имя_таблицы>')
                         continue
                     table_name = args[0]
                     db_meta = drop_table(db_meta, table_name)
                     save_metadata(DB_METADATA_FILE, db_meta)
                  case 'list_tables':
                     tables = list_tables(db_meta)
                     if tables:
                         print('Список таблиц:')
                         for table in tables:
                             print(f'-> {table}')
                     else:
                         print('Таблиц пока нет.')
                  case _:
                     print(f'Команды {command} нет. Попробуйте снова.')
       except ValueError as e:
           print(f'Ошибка значения: {e}')
       except TypeError as e:
           print(f'Ошибка типа: {e}')
       except Exception as e:
           print(f'Произошла непредвиденная ошибка: {e}')