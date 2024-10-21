import tkinter as tk
from tkinter import messagebox
import random
import string

# Функция для усложнения пароля
def enhance_password(password):
    min_length = 8
    max_length = 16
    require_uppercase = True
    require_lowercase = True
    require_digits = True
    require_special = True

    # Добавление необходимых символов
    if require_uppercase and not any(c.isupper() for c in password):
        password += random.choice(string.ascii_uppercase)
    if require_lowercase and not any(c.islower() for c in password):
        password += random.choice(string.ascii_lowercase)
    if require_digits and not any(c.isdigit() for c in password):
        password += random.choice(string.digits)
    if require_special and not any(c in string.punctuation for c in password):
        password += random.choice(string.punctuation)

    # Удлинение пароля до минимальной длины
    while len(password) < min_length:
        password += random.choice(string.ascii_letters + string.digits + string.punctuation)

    # Обрезка пароля до максимальной длины
    if len(password) > max_length:
        password = password[:max_length]

    return password

# Функция для обработки ввода пользователя
def process_password():
    desired_password = entry.get()
    if not desired_password:
        messagebox.showerror("Ошибка", "Введите желаемый пароль")
        return

    enhanced_password = enhance_password(desired_password)

    # Отображение усложненного пароля
    result_label.config(text=f"Усложненный пароль: {enhanced_password}")

# Создание GUI
root = tk.Tk()
root.title("Генератор паролей")

frame = tk.Frame(root)
frame.pack(padx=10, pady=10)

label = tk.Label(frame, text="Введите желаемый пароль:")
label.pack(pady=5)

entry = tk.Entry(frame, show="*")
entry.pack(pady=5)

generate_button = tk.Button(frame, text="Усложнить пароль", command=process_password)
generate_button.pack(pady=5)

result_label = tk.Label(frame, text="")
result_label.pack(pady=5)

root.mainloop()