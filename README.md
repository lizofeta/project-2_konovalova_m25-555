# project-2_konovalova_m25-555

# primitive database 

## Описание 

Это примитивная консольная база данных, написанная на Python. 
Поддерживаются команды по созданию, управлению и удалению таблиц.

## Установка 

1. Установите pip и poetry (если еще не установлен) 
```bash
sudo apt install python3-pip
sudo apt install python3-poetry
```

2. Клонируйте репозиторий (если работаете с git) и перейдите в директорию проекта

Клонирование:
```bash
git clone https://github.com/lizofeta/project-2_konovalova_m25-555.git
```

Переход в директорию проекта:
```bash
cd project-2_konovalova_m25-555
```

Если вы скачали zip-архив, распакуйте его в удобное для вас место. 
Затем откройте терминал и перейдите в директорию проекта:
```bash
cd [путь_к_директории_проекта]
```

3. Установите зависимости проекта 
```bash
poetry install
```

## Запуск

Запустить базу данных можно по команде:
```bash
make database
```

## Управление таблицами

Возможные команды:

- <command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - создать таблицу
- <command> list_tables - показать список всех таблиц
- <command> drop_table <имя_таблицы> - удалить таблицу
- <command> exit - выход из программы
- <command> help - справочная информация

## Asciinema 
Пример использования в записи
- Установка пакета 
- Запуск базы данных
- Проверка таблиц
- Создание таблицы
- Удаление таблицы

[![asciicast](https://asciinema.org/a/lSUarc50DaorhZybwLOVScBSG.svg)](https://asciinema.org/a/lSUarc50DaorhZybwLOVScBSG)

Link: https://asciinema.org/a/lSUarc50DaorhZybwLOVScBSG