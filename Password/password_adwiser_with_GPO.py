import tkinter as tk
from tkinter import messagebox
import random
import string
import win32com.client

# Словарь для замены букв на схожие специальные символы
char_replacements = {
    'a': '@', 'A': '@',
    'b': '8', 'B': '8',
    'e': '3', 'E': '3',
    'i': '!', 'I': '!',
    'l': '1', 'L': '1',
    'o': '0', 'O': '0',
    's': '$', 'S': '$',
    't': '7', 'T': '7'
}

# Функция для получения политики паролей
def get_password_policy():
    policy = {
        'min_length': 8,
        'complexity': True
    }
    try:
        client = win32com.client.Dispatch("WbemScripting.SWbemLocator")
        service = client.ConnectServer(".", "root\cimv2")
        items = service.ExecQuery("SELECT * FROM Win32_AccountPolicy")
        for item in items:
            policy['min_length'] = item.MinPasswordLength
            policy['complexity'] = item.PasswordComplexity
    except Exception as e:
        messagebox.showerror("Ошибка", f"Не удалось получить политику паролей: {e}")
    return policy

# Функция для изменения пароля
def modify_password():
    user_password = entry.get()
    if not user_password:
        messagebox.showerror("Ошибка", "Введите пароль")
        return

    policy = get_password_policy()

    # Замена букв на схожие специальные символы
    modified_password = ''.join(char_replacements.get(char, char) for char in user_password)

    # Случайная замена регистра букв
    modified_password = ''.join(
        char.upper() if random.choice([True, False]) else char.lower() for char in modified_password
    )

    # Проверка длины пароля и добавление символов, если необходимо
    while len(modified_password) < policy['min_length']:
        modified_password += random.choice(string.ascii_letters + string.digits + string.punctuation)

    # Проверка на наличие цифр, букв и специальных символов
    if policy['complexity']:
        if not any(char.isdigit() for char in modified_password):
            modified_password += random.choice(string.digits)
        if not any(char.isalpha() for char in modified_password):
            modified_password += random.choice(string.ascii_letters)
        if not any(char in string.punctuation for char in modified_password):
            modified_password += random.choice(string.punctuation)

    # Перемешивание символов для большей безопасности
    modified_password = list(modified_password)
    random.shuffle(modified_password)
    modified_password = ''.join(modified_password)

    messagebox.showinfo("Измененный пароль", f"Ваш измененный пароль: {modified_password}")

# Создание GUI
root = tk.Tk()
root.title("Изменение пароля")

label = tk.Label(root, text="Введите пароль:")
label.pack(pady=10)

entry = tk.Entry(root, show="*", width=30)
entry.pack(pady=10)

button = tk.Button(root, text="Изменить пароль", command=modify_password)
button.pack(pady=10)

root.mainloop()