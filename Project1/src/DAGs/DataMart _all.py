# Импортируем библиотеки
import pandahouse as ph
import clickhouse_connect
import pandas as pd
import numpy as np
import clickhouse2pandas as ch2pd

# Настраиваем подключение к БД
connection_url = 'http://default:@localhost:8123'
query1 = 'select * from vedomostiRSS limit 10000'
query2 = 'select * from lentaRSS limit 10000'
query3 = 'select * from tassRSS limit 10000'

# Забираем данные в Датафреймы
Vedomosti= ch2pd.select(connection_url, query1)
Vedomosti.insert(0, "Source", 'Vedomosti')
Lenta= ch2pd.select(connection_url, query2)
Lenta.insert(0, "Source", 'Lenta')
Tass= ch2pd.select(connection_url, query3)
Tass.insert(0, "Source", 'Tass')

# Объединяем в общую базу
Temp = pd.merge(Vedomosti,Lenta, how='outer')
News = pd.merge(Temp,Tass, how='outer').sort_values('published')

 # Общее количество новостей по категориям за все время со всех источников
total_category_count = News['category_id'].value_counts()

# Общее количество новостей по категориям за все время с источника Ведомости
total_category_count_vedomosti = Vedomosti['category_id'].value_counts()

 # Общее количество новостей по категориям за все время с источника Лента
total_category_count_lenta = Lenta['category_id'].value_counts()

# Общее количество новостей по категориям за все время с источника Тасс
total_category_count_tass = Tass['category_id'].value_counts()

# Выводим количество публикаций по категориям за последнии сутки с источника Ведомости
day_category_count_vedomosti= Vedomosti[(Vedomosti['published'] < Vedomosti['published'].values.max()) & (Vedomosti['published'] > (Vedomosti['published'].values.max() - pd.DateOffset(days=1)) )]['category_id'].value_counts()

# Выводим количество публикаций по категориям за последнии сутки с источника Лента
day_category_count_lenta= Lenta[(Lenta['published'] < Lenta['published'].values.max()) & (Lenta['published'] > (Lenta['published'].values.max() - pd.DateOffset(days=1)) )]['category_id'].value_counts()

# Выводим количество публикаций по категориям за последнии сутки с источника Тасс
day_category_count_tass= Tass[(Tass['published'] < Tass['published'].values.max()) & (Tass['published'] > (Tass['published'].values.max() - pd.DateOffset(days=1)) )]['category_id'].value_counts()

# Выводим среднее количество публикаций по категориям в сутки
total_category_count_byDay = []
mean_category_daily = []
max_category_count_day = []
category_count_byWeekDay = []
for i in [0, 1, 2, 3, 4, 5, 6, 7]:
 total_category_count_byDay.append(News[News['category_id'] == i]["published"].dt.date.value_counts().tolist())
 mean_category_daily.append(sum(total_category_count_byDay[i])/len(total_category_count_byDay[i]))
 # Выведем день в который было максимально колличество публикаций по категориям
 max_category_count_day.append(News[News['category_id'] == i]["published"].dt.date.value_counts().nlargest(1).index[0])
 # Количество публикаций по категориям по дням недели
 category_count_byWeekDay.append(News[News['category_id'] == i]["published"].dt.day_name().value_counts())
 count_byWeekDay = pd.DataFrame(category_count_byWeekDay)
 count_byWeekDay = count_byWeekDay.reset_index(drop=True)

# Создаем витрину данных
variables = {'category_id':[0, 1, 2, 3, 4, 5, 6, 7],
              'category_name': ['Разное', 'Политика', 'Общество', 'Экономика', 'Медиа и СМИ', 'Технологии', 'Спорт и здоровье', 'Россия и бывший СССР'],
             'total_category_count':total_category_count,
             'total_category_count_vedomosti':total_category_count_vedomosti,
             'total_category_count_lenta':total_category_count_lenta,
             'total_category_count_tass':total_category_count_tass,
             'day_category_count_vedomosti':day_category_count_vedomosti,
             'day_category_count_lenta':day_category_count_lenta,
             'day_category_count_tass':day_category_count_tass,
             'mean_category_daily':mean_category_daily,
             'max_category_count_day':max_category_count_day,
             'category_count_Monday':count_byWeekDay['Monday'],
             'category_count_Tuesday':count_byWeekDay['Tuesday'],
             'category_count_Wednesday':count_byWeekDay['Wednesday'],
             'category_count_Thursday':count_byWeekDay['Thursday'],
             'category_count_Friday':count_byWeekDay['Friday'],
             'category_count_Saturday':count_byWeekDay['Saturday'],
             'category_count_Sunday':count_byWeekDay['Sunday']}
DataMart = pd.DataFrame(variables)

DataMart.set_index('category_id', inplace=True)
DataMart = DataMart.T

print(DataMart)