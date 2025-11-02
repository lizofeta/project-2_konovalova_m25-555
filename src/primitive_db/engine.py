import prompt


def _print_commands():
     print('<command> exit - выйти из программы\n' 
           '<command> help - справочная информация')

def welcome():
    print('\nПервая попытка запустить проект!\n\n')
    print('***')

    _print_commands()

    while True:
       command = prompt.string('Введите команду: ').lower()

       if command == 'exit':
              break
       elif command == 'help':
              _print_commands()
       else:
            print('Неизвестная команда.')