slovo = str(input('Введите слово: '))
a = slovo.replace(" ", "").lower()
if a == a[::-1]:
    print("true")
else:
    print("false")