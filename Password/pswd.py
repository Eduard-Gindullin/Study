import tkinter as tk
from tkinter import messagebox
import random
import string

# Политика паролей
password_policy = {
    "min_length": 8,
    "complexity": True
}

# Функция для замены букв на похожие спецсимволы и случайного изменения регистра
def transform_password(password):
    replacements = {
        'a': '@', 'A': '4', 'b': '8', 'B': '8', 'e': '3', 'E': '3',
        'i': '!', 'I': '1', 'o': '0', 'O': '0', 's': '$', 'S': '$',
        't': '7', 'T': '7'
    }
    transformed = ''.join(replacements.get(c, random.choice([c.lower(), c.upper()])) if c in replacements else random.choice([c.lower(), c.upper()]) for c in password)
    return transformed

# Функция для генерации пароля на основе случайных слов
def generate_password(bits=40):
    words = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew"]
    if bits == 40:
        password = ''.join(random.choice(words) for _ in range(2))
    elif bits == 128:
        password = ''.join(random.choice(words) for _ in range(4))
    elif bits == 256:
        password = ''.join(random.choice(words) for _ in range(8))
    transformed_password = transform_password(password)
    password_entry.delete(0, tk.END)
    password_entry.insert(0, transformed_password)
    result_label.config(text=f"Generated Password: {transformed_password}")

# Функция для обработки введенного пароля
def process_password():
    password = password_entry.get()
    if not password:
        messagebox.showwarning("Warning", "Please enter a password")
        return
    if len(password) < password_policy["min_length"]:
        password += ''.join(random.choice(string.ascii_letters + string.digits + string.punctuation) for _ in range(password_policy["min_length"] - len(password)))
    transformed_password = transform_password(password)
    result_label.config(text=f"Transformed Password: {transformed_password}")

# Функция для генерации пароля по политикам
def generate_policy_password():
    words = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew"]
    password = ''
    while len(password) < password_policy["min_length"]:
        word = random.choice(words)
        if len(password) + len(word) <= password_policy["min_length"]:
            password += word
        else:
            break
    # Проверка на минимальную длину и добавление символов при необходимости
    if len(password) < password_policy["min_length"]:
        password += ''.join(random.choice(string.ascii_letters + string.digits + string.punctuation) for _ in range(password_policy["min_length"] - len(password)))
    transformed_password = transform_password(password)
    password_entry.delete(0, tk.END)
    password_entry.insert(0, transformed_password)
    result_label.config(text=f"Generated Policy Password: {transformed_password}")

# Создание основного окна
root = tk.Tk()
root.title("Password Transformer")
root.geometry("900x300")  # Увеличиваем ширину окна в 3 раза

# Поле для ввода пароля
password_label = tk.Label(root, text="Enter Password:")
password_label.pack(pady=5)
password_entry = tk.Entry(root, show="")  # Убираем маскировку пароля
password_entry.pack(pady=5)

# Кнопка для обработки пароля
process_button = tk.Button(root, text="Transform Password", command=process_password)
process_button.pack(pady=5)

# Кнопка для генерации пароля
generate_button_40 = tk.Button(root, text="Generate 40-bit Password", command=lambda: generate_password(40))
generate_button_40.pack(pady=5)

generate_button_128 = tk.Button(root, text="Generate 128-bit Password", command=lambda: generate_password(128))
generate_button_128.pack(pady=5)

generate_button_256 = tk.Button(root, text="Generate 256-bit Password", command=lambda: generate_password(256))
generate_button_256.pack(pady=5)

# Кнопка для генерации пароля по политикам
generate_policy_button = tk.Button(root, text="Generate Policy Password", command=generate_policy_password)
generate_policy_button.pack(pady=5)

# Метка для отображения результата
result_label = tk.Label(root, text="")
result_label.pack(pady=5)

# Запуск основного цикла
root.mainloop()