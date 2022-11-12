import re
exp = str(input('Введите выражение: '))
exp_mod = ''.join([el for el in exp if el in ('(',')','{','}','[',']')]) #убираем все кроме скобок
if len(exp_mod) % 2 != 0:
    print(False) #если нечетно количество - сразу в утиль
elif len(re.findall(r'\((?=[\]\}])|\[(?=[\)\}])|\{(?=[\)\]])', exp_mod)) > 0:
    print(False) #если после открытия сразу есть иное закрытие (например, после '{' идет ']')- тоже в утиль
else: # ищем соотв пары '()','[]','{}' и те же пары, только с 2, 4 и далее другими скобками м/у ними - ну тут напутано, можно и попроще
    pattern = r'\(\)|\[\]|\{\}|\(.{2}\)|\[.{2}\]|\{.{2}\}|\(.{4}\)|\[.{4}\]|\{.{4}\}|\(.{' + str(len(exp_mod)-4) + '}\)|\[.{' + str(len(exp_mod)-4) + '}\]|\{.{' + str(len(exp_mod)-4) + '}\}|\(.{' + str(len(exp_mod)-2) + '}\)|\[.{' + str(len(exp_mod)-2) + '}\]|\{.{' + str(len(exp_mod)-2) + '}\}|\(.{' + str(len(exp_mod)-8) + '}\)|\[.{' + str(len(exp_mod)-8) + '}\]|\{.{' + str(len(exp_mod)-8) + '}\}'
    if len(re.sub(pattern, '', exp_mod)) > 0:
        print(False)            
    else:
        print(True)