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

#print(df5)
#print(df5['published'].dt.day_name())

#agg_func_count = {'embark_town': ['count', 'nunique', 'size']}
#df6 = df5.groupby(['tags'])
# Общее количество новостей по категории Россия и бывший СССР за все время со всех источников
total_category_count = df5['tags'].value_counts().iloc[2]
# Общее количество новостей по категории Россия и бывший СССР за все время с источника Ведомости
total_category_count_vedomosti = df1['tags'].value_counts().iloc[2]
# Общее количество новостей по категории Россия и бывший СССР за все время с источника Лента
total_category_count_lenta = df2['tags'].value_counts().iloc[2]
# Общее количество новостей по категории Россия и бывший СССР за все время с источника Тасс
total_category_count_tass = df3['tags'].value_counts().iloc[2]
# Узнаем дату нашей последней публикации
time_last_published = df5['published'].values.max()
# Выводим количество публикаций по категории Россия и бывший СССР за последнии сутки с источника Ведомости
day_category_count_vedomosti= df1[(df1['published'] < df1['published'].values.max()) & (df1['published'] > (df1['published'].values.max() - pd.DateOffset(days=1)) )]['tags'].value_counts().iloc[2]
# Выводим количество публикаций по категории Россия и бывший СССР за последнии сутки с источника Лента
day_category_count_lenta= df2[(df2['published'] < df2['published'].values.max()) & (df2['published'] > (df2['published'].values.max() - pd.DateOffset(days=1)) )]['tags'].value_counts().iloc[2]
# Выводим количество публикаций по категории Россия и бывший СССР за последнии сутки с источника Тасс
day_category_count_tass= df3[(df3['published'] < df3['published'].values.max()) & (df3['published'] > (df3['published'].values.max() - pd.DateOffset(days=1)) )]['tags'].value_counts().iloc[2]

print(day_category_count_tass)

""" published = []
published.append(client.command('Select DISTINCT title, published from vedomosti'))
data_list2 = []
for j in published:
  data_list2.append([j["title"],j["published"]])
df1 = pd.DataFrame(data_list2, columns=['title',"published"]) """