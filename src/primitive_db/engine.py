import os
import shlex

import prompt

from src.decorators import handle_db_errors
from src.primitive_db.constants import DB_METADATA_FILE
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
from src.primitive_db.parser import (
     insert_columns_parser,
     set_clause_parser,
     where_clause_parser,
)
from src.primitive_db.utils import (
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

@handle_db_errors
def run():
    print('\nДобро пожаловать в примитивную базу данных!\n\n')
    print('***')
    print('Для получения списка команд введите help')

    while True:
        db_meta = load_metadata(DB_METADATA_FILE)
        if db_meta is None:
            print(('ошибка при выгрузке метаданных. '
            'Возможно, файл поврежден или отсутствует.\n'
            'Выход из программы.'))
            break
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
                        print(('Передано '
                        'недостаточное количество аргументов.\n'
                        'Ожидается минимум 2: <имя_таблицы> <имя_столбца:тип>.\n'
                        'Попробуйте снова.'))
                        continue
                    table_name = args[0]
                    raw_columns = args[1:]
                    columns = insert_columns_parser(raw_columns)
                    db_meta = create_table(db_meta, table_name, columns)
                    if db_meta is not None:
                        save_metadata(DB_METADATA_FILE, db_meta)
                        save_table_data(table_name + '.json', db_meta[table_name])
                    else:
                        print('Таблица не была создана.')
                case 'drop_table':
                    if len(args) != 1:
                        print('Передано неверное число аргументов.\n'
                        'Ожидается 1: <имя_таблицы>\nПопробуйте снова.')
                        continue
                    table_name = args[0]
                    table_filepath = table_name + '.json'
                    db_meta = drop_table(db_meta, table_name)
                    if db_meta is not None:
                        save_metadata(DB_METADATA_FILE, db_meta)
                        os.remove(table_filepath) 
                        print(f'Таблица с именем "{table_name}" успешно удалена.')
                case 'list_tables':
                    if len(args) > 0:
                        print(('Неверный ввод команды.\n'
                        'Ожидается: list_tables\nПопробуйте снова.'))
                        continue
                    tables = list_tables(db_meta)
                    if tables:
                        print('Список таблиц:')
                        for table in tables:
                            print(f'-> {table}')
                    else:
                        print('Таблиц пока нет.')
                case 'select':
                    if len(args) < 2:
                        print('Неверный ввод команды.\nПопробуйте снова')
                        continue
                    if 'from' not in args:
                        print(('В запросе "select" должно '
                        'фигурировать слово "from".\n'
                        'Попробуйте снова.'))
                        continue
                    args = args[1:]
                    if len(args) < 1:
                        print('Передано недостаточное'
                        ' количество аргументов. '
                        'Ожидается минимум 1.\nПопробуйте снова.')
                        continue
                    table_name = args[0]
                    table_filepath = table_name + '.json'
                    table = load_table_data(table_filepath)
                    if table is not None:
                        if 'where' in args:
                            where_clause = where_clause_parser(args[2:])
                            table = select(table, where_clause) 
                            print(table)
                        else:
                            table = select(table)
                            print(table)
                    else:
                        print('Ошибка чтения таблицы.')
                case 'insert':
                    if len(args) < 4:
                        print('Неверный ввод команды. Попробуйте снова.')
                        continue
                    if 'into' != args[0] or 'values' != args[2]:
                        print('В команде "insert" '
                        'должны фигурировать слова "into" и "values".')
                        continue
                    table_name = args[1]
                    table_filepath = table_name + '.json'
                    table_data = load_table_data(table_filepath)
                    values = args[3:]
                    if '(' not in values[0] or ')' not in values[-1]:
                        print(('Значения для добавления '
                        'в таблицу должны быть записаны '
                        'в скобках: (<значение1>, <значение2> ...)'))
                        continue
                    processed_values = [value.strip('(').strip(')').strip(',') for value in values] #noqa: E501
                    db_meta = insert(db_meta, table_name, processed_values)
                    if db_meta is not None:
                        table_data = db_meta[table_name]
                        save_metadata(DB_METADATA_FILE, db_meta)
                        save_table_data(table_filepath, table_data)
                case 'update':
                    if len(args) != 9:
                        print('Неверный ввод команды. Попробуйте снова.')
                        continue
                    if 'set' != args[1] or 'where' != args[5]:
                        print(('В команде "update" должны '
                        'фигурировать слова "set" и "where".\n'
                        'Попробуйте снова.'))
                        continue
                    table_name = args[0]
                    table_filepath = table_name + '.json'
                    table_data = load_table_data(table_filepath)
                    set_clause = args[2:5]
                    if '=' not in set_clause or 'where' == set_clause[2]:
                        print('Неверный ввод команды. Попробуйте снова.')
                        continue
                    where_clause = args[6:]
                    if len(where_clause) != 3 or '=' not in where_clause:
                        print('Неверный ввод команды. Попробуйте снова.')
                        continue
                    set_clause = set_clause_parser(set_clause)
                    where_clause = where_clause_parser(where_clause)
                    if table is not None:
                        updated_table = update(table_data, set_clause, where_clause)
                        db_meta[table_name] = updated_table
                        save_table_data(table_filepath, updated_table)
                        save_metadata(DB_METADATA_FILE, db_meta)
                        print(show_table(updated_table))
                    else:
                        print('Не удалось обновить запись.')
                case 'delete':
                    if len(args) != 6:
                        print("Неверный ввод команды. Попробуйте снова.")
                        continue
                    if 'from' != args[0] or 'where' != args[2] or '=' != args[4]:
                        print('Неверный ввод команды. Попробуйте снова.')
                        continue
                    table_name = args[1]
                    table_filepath = table_name + '.json'
                    table_data = load_table_data(table_filepath)
                    where_clause = args[3:]
                    if len(where_clause) != 3:
                        print('Неверный ввод команды. Попробуйте снова.')
                        continue
                    where_clause = where_clause_parser(where_clause)
                    if table_data is not None:
                        updated_table = delete(table_data, where_clause)
                        db_meta[table_name] = updated_table
                        save_table_data(table_filepath, updated_table) 
                        save_metadata(DB_METADATA_FILE, db_meta)
                        print('Запись успешно удалена. Обновленная таблица: ')
                        print(show_table(updated_table)) 
                case 'info':
                    if len(args) != 1:
                        print(('Передано неверное количество аргументов. '
                        'Ожидается 1: <имя_таблицы>\nПопробуйте снова.'))
                        continue
                    table_name = args[0]
                    table_filepath = table_name + '.json'
                    table_data = load_table_data(table_filepath)
                    if table_data is not None:
                        info(table_data, table_name)
                case _:
                    print(f'Команды {command} нет. Попробуйте снова.')