import pandahouse as ph
import clickhouse_connect
import pandas as pd

#print(data_list4)

import clickhouse2pandas as ch2pd

connection_url = 'http://default:@localhost:8123'
query1 = 'select * from vedomostiRSS limit 10000'
query2 = 'select * from lentaRSS limit 10000'
query3 = 'select * from tassRSS limit 10000'


df1= ch2pd.select(connection_url, query1)
df2= ch2pd.select(connection_url, query2)
df3= ch2pd.select(connection_url, query3)


df4 = pd.merge(df1,df2, how='outer')
df5 = pd.merge(df4,df3, how='outer')






# Общее количество новостей по категории Общество за все время со всех источников
total_category_count = df5['category_id'].value_counts()[2]

# Общее количество новостей по категории Общество за все время с источника Ведомости
total_category_count_vedomosti = df1['category_id'].value_counts()[2]

# Общее количество новостей по категории Общество за все время с источника Лента
total_category_count_lenta = df2['category_id'].value_counts()[2]

# Общее количество новостей по категории Общество за все время с источника Тасс
total_category_count_tass = df3['category_id'].value_counts()[2]

# Выводим количество публикаций по категории Общество за последнии сутки с источника Ведомости
day_category_count_vedomosti= df1[(df1['published'] < df1['published'].values.max()) & (df1['published'] > (df1['published'].values.max() - pd.DateOffset(days=1)) )]['category_id'].value_counts()[2]

# Выводим количество публикаций по категории Общество за последнии сутки с источника Лента
day_category_count_lenta= df2[(df2['published'] < df2['published'].values.max()) & (df2['published'] > (df2['published'].values.max() - pd.DateOffset(days=1)) )]['category_id'].value_counts()[2]

# Выводим количество публикаций по категории Общество за последнии сутки с источника Тасс
day_category_count_tass= df3[(df3['published'] < df3['published'].values.max()) & (df3['published'] > (df3['published'].values.max() - pd.DateOffset(days=1)) )]['category_id'].value_counts()[2]

# Выводим среднее количество публикаций по категории Общество в сутки
total_category_count_byDay = df5[df5['category_id'] == 2]["published"].dt.date.value_counts().tolist()
mean_category_daily = sum(total_category_count_byDay)/len(total_category_count_byDay)

# Выведем день в который было максимально колличество публикаций по категории Общество
max_category_count_day = df5[df5['category_id'] == 2]["published"].dt.date.value_counts().nlargest(1)

# Количество публикаций по теме Общество по дням недели
category_count_byWeekDay = df5[df5['category_id'] == 2]["published"].dt.day_name().value_counts()

# Создаем витрину
variables = {'category_id':2,
             'tags':'Общество',
             'total_category_count':total_category_count,
             'total_category_count_vedomosti':total_category_count_vedomosti,
             'total_category_count_lenta':total_category_count_lenta,
             'total_category_count_tass':total_category_count_tass,
             'day_category_count_vedomosti':day_category_count_vedomosti,
             'day_category_count_lenta':day_category_count_lenta,
             'day_category_count_tass':day_category_count_tass,
             'mean_category_daily':mean_category_daily,
             'max_category_count_day':max_category_count_day,
             'category_count_byWeekDay':category_count_byWeekDay}

DataMart = pd.DataFrame(variables, index=[1])

#new_row = {'name':'Geo', 'physics':87, 'chemistry':92, 'algebra':97} #append row to the dataframe df_marks = df_marks.append(new_row, ignore_index=True)
#df_marks = df_marks.append(new_row, ignore_index=True)

print(max_category_count_day)
#print(df6)

""" published = []
published.append(client.command('Select DISTINCT title, published from vedomosti'))
data_list2 = []
for j in published:
  data_list2.append([j["title"],j["published"]])
df1 = pd.DataFrame(data_list2, columns=['title',"published"]) """