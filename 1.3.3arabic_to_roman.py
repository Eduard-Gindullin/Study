number = int(input('Введите число от 0 до 2000: '))
if number < 0:
    print("Ваше число должно быть больше 0")
elif number > 2000:
    print("Ваше число слишком большое")
else:
   def printRoman(number):
    num = [1, 4, 5, 9, 10, 40, 50, 90,
        100, 400, 500, 900, 1000]
    sym = ["I", "IV", "V", "IX", "X", "XL",
        "L", "XC", "C", "CD", "D", "CM", "M"]
    i = 12
      
    while number:
        div = number // num[i]
        number %= num[i]
  
        while div:
            print(sym[i], end = "")
            div -= 1
        i -= 1
  
# Driver code
if __name__ == "__main__":
    
    print("Римское число:", end = " ")
    printRoman(number)
