import os
import shlex

import prompt

from src.primitive_db.core import (
     create_table,
     delete,
     drop_table,
     info,
     insert,
     list_tables,
     select,
     update,
)
from src.primitive_db.parser import set_clause_parser, where_clause_parser
from src.primitive_db.utils import (
     DB_METADATA_FILE,
     load_metadata,
     load_table_data,
     save_metadata,
     save_table_data,
     show_table,
)


def help():
     print('\n*** Процесс работы с таблицей ***')

     print('\nКоманды для работы с таблицами:')
     print(('create_table <имя_таблицы> '
     '<столбец1:тип> <столбец2:тип> .. - создать таблицу'))
     print('list_tables - показать список всех таблиц')
     print('drop_table <имя_таблицы> - удалить таблицу')
     print(('insert into <имя_таблицы> '
     'values (<значение1>, <значение2>, ...) - создать запись.')) 
     print(('select from <имя_таблицы> '
     'where <столбец> = <значение> - прочитать записи по условию.')) 
     print('select from <имя_таблицы> - прочитать все записи.')
     print(('update <имя_таблицы> '
     'set <столбец1> = <новое_значение1> '
     'where <столбец_условия> = <значение_условия> - обновить запись.'))
     print(('delete from <имя_таблицы> '
     'where <столбец> = <значение> - удалить запись.'))
     print('info <имя_таблицы> - вывести информацию о таблице.')

     print('\nОбщие команды:')
     print('exit - выйти из программы')
     print('help - справочная информация')

def run():
    print('\nДобро пожаловать в примитивную базу данных!\n\n')
    print('***')
    print('Для получения списка команд введите help')

    while True:
       try:
           db_meta = load_metadata(DB_METADATA_FILE)
           raw_command = prompt.string('\nВведите команду: ').strip()
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
                         raise ValueError(('Передано '
                         'недостаточное количество аргументов.\n'
                         'Ожидается минимум 2: <имя_таблицы> <имя_столбца:тип>'))
                     table_name = args[0]
                     raw_columns = args[1:]
                     columns_to_add = []
                     for col in raw_columns:
                         if ':' not in col:
                            raise ValueError(('Неверный формат ' 
                            'записи описания столбца.\n'
                            'Ожидается: <имя_столбца:тип>'))
                         columns_to_add.append(col)
                     db_meta = create_table(db_meta, table_name, columns_to_add)
                     save_metadata(DB_METADATA_FILE, db_meta)
                     save_table_data(table_name + '.json', db_meta[table_name])
                  case 'drop_table':
                     if len(args) != 1:
                         raise ValueError(('Передано неверное число аргументов.\n'
                         'Ожидается 1: <имя_таблицы>'))
                     table_name = args[0]
                     table_filepath = table_name + '.json'
                     db_meta = drop_table(db_meta, table_name)
                     save_metadata(DB_METADATA_FILE, db_meta)
                     os.remove(table_filepath) 
                  case 'list_tables':
                     tables = list_tables(db_meta)
                     if tables:
                         print('Список таблиц:')
                         for table in tables:
                             print(f'-> {table}')
                     else:
                         print('Таблиц пока нет.')
                  case 'select':
                     if 'from' not in args:
                         raise ValueError((('В запросе "select" должно '
                         'фигурировать слово "from".\n'
                         'Попробуйте снова.')))
                     args = args[1:]
                     if len(args) < 1:
                         raise ValueError(('Передано недостаточное'
                         ' количество аргументов. Ожидается минимум 1.'))
                     table_name = args[0]
                     table_filepath = table_name + '.json'
                     table = load_table_data(table_filepath)
                     if 'where' in args:
                         where_clause = where_clause_parser(args[2:])
                         table = select(table, where_clause) 
                         print(table)
                     else:
                         table = select(table)
                         print(table)
                  case 'insert':
                     if 'into' not in args or 'values' not in args:
                         raise ValueError(('В команде "insert" '
                         'должно фигурировать слова "into" и "values".'))
                     table_name = args[1]
                     table_filepath = table_name + '.json'
                     table_data = load_table_data(table_filepath)
                     values = args[3:]
                     if '(' not in values[0] or ')' not in values[-1]:
                         raise ValueError(('Значения для добавления '
                         'в таблицу должны быть записаны '
                         'в скобках: (<значение1>, <значение2> ...)'))
                     processed_values = [value.strip('(').strip(')').strip(',') for value in values] #noqa: E501
                     db_meta = insert(db_meta, table_name, processed_values)
                     table_data = db_meta[table_name]
                     save_metadata(DB_METADATA_FILE, db_meta)
                     save_table_data(table_filepath, table_data)
                  case 'update':
                     if 'set' != args[1] or 'where' != args[5]:
                         raise ValueError(('В команде "update" должны '
                         'фигурировать слова "set" и "where".\n'
                         'Попробуйте снова.'))
                     table_name = args[0]
                     table_filepath = table_name + '.json'
                     table_data = load_table_data(table_filepath)
                     set_clause = args[2:5]
                     if '=' not in set_clause or 'where' == set_clause[2]:
                         raise ValueError('Неверный ввод команды. Попробуйте снова.')
                     where_clause = args[6:]
                     if len(where_clause) != 3 or '=' not in where_clause:
                         raise ValueError('Неверный ввод команды. Попробуйте снова.')
                     set_clause = set_clause_parser(set_clause)
                     where_clause = where_clause_parser(where_clause)
                     updated_table = update(table_data, set_clause, where_clause)
                     db_meta[table_name] = updated_table
                     save_table_data(table_filepath, updated_table)
                     save_metadata(DB_METADATA_FILE, db_meta)
                     print(show_table(updated_table))
                  case 'delete':
                     if 'from' not in args or 'where' not in args or '=' not in args:
                         raise ValueError('Неверный ввод команды. Попробуйте снова.')
                     table_name = args[1]
                     table_filepath = table_name + '.json'
                     table_data = load_table_data(table_filepath)
                     where_clause = args[3:]
                     if len(where_clause) != 3:
                         raise ValueError('Неверный ввод команды. Попробуйте снова.')
                     where_clause = where_clause_parser(where_clause)
                     updated_table = delete(table_data, where_clause)
                     db_meta[table_name] = updated_table
                     save_table_data(table_filepath, updated_table) 
                     save_metadata(DB_METADATA_FILE, db_meta)
                     print(show_table(updated_table)) 
                  case 'info':
                   if len(args) != 1:
                       raise ValueError(('Передано неверное количество аргументов. '
                       'Ожидается 1: <имя_таблицы>'))
                   table_name = args[0]
                   table_filepath = table_name + '.json'
                   table_data = load_table_data(table_filepath)
                   info(table_data, table_name)
                  case _:
                     print(f'Команды {command} нет. Попробуйте снова.')
       except ValueError as e:
           print(f'Ошибка значения: {e}')
       except TypeError as e:
           print(f'Ошибка типа: {e}')
       except Exception as e:
           print(f'Произошла непредвиденная ошибка: {e}')