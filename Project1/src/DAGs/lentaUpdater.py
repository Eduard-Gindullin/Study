import pandahouse as ph
import feedparser
import clickhouse_connect
import pandas as pd
import numpy as np

client = clickhouse_connect.get_client(host='localhost', username='default', password='')

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')
last_published = client.command('Select * from lentaRSS where published IN (SELECT MAX(`published`) as `time` FROM `lentaRSS`)')
time = last_published[4]

d = feedparser.parse('https://lenta.ru/rss/')
data_list = []
for i in d['entries']:
    data_list.append([i["summary"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["summary","link","tags","published"])
df['published'] = df['published'].astype('datetime64[ns]')
df = df.rename(columns={'summary': 'title'})

conditions = [(df['tags'] == 'Политика') , (df['tags'] == 'Общество'), (df['tags'] == 'Бизнес'), (df['tags'] == 'Экономика'), (df['tags'] == 'Финансы'), (df['tags'] == 'Медиа'), (df['tags'] == 'Авто'), (df['tags'] == 'Политика / Власть'), 
(df['tags'] == 'Политика / Международные отношения'), (df['tags'] == 'Технологии'), (df['tags'] == 'Среда обитания'), (df['tags'] == 'Недвижимость'), (df['tags'] == 'Экономика и бизнес'), (df['tags'] == 'Армия и ОПК'), (df['tags'] == 'Происшествия'),
(df['tags'] == 'Культура'), (df['tags'] == 'НедвижимостьОбщество'), (df['tags'] == 'Международная панорама'), (df['tags'] == 'Спорт'), (df['tags'] == 'Москва'), (df['tags'] == 'Северный Кавказ'), (df['tags'] == 'Космос'), (df['tags'] == 'Биографии и справки'),
(df['tags'] == 'Военная операция на Украине'), (df['tags'] == 'Мир'), (df['tags'] == 'Забота о себе'), (df['tags'] == 'Россия'), (df['tags'] == 'Путешествия'), (df['tags'] == 'Ценности'), (df['tags'] == 'Бывший СССР'), (df['tags'] == 'Интернет и СМИ'),
(df['tags'] == 'Силовые структуры'), (df['tags'] == 'Наука и техника'), (df['tags'] == 'Из жизни'), (df['tags'] == 'Моя страна'), (df['tags'] == 'Национальные проекты'), (df['tags'] == 'Новости партнеров'), (df['tags'] == 'Новости регионов'), (df['tags'] == '69-я параллель')]

choices = ['Политика', 'Общество', 'Экономика', 'Экономика', 'Экономика', 'Медиа и СМИ', 'Технологии', 'Политика', 'Политика', 'Технологии', 'Общество', 'Экономика', 'Экономика', 'Технологии', 'Общество', 'Общество', 'Экономика', 'Политика', 'Спорт и здоровье', 
'Россия и бывший СССР', 'Россия и бывший СССР', 'Технологии', 'Общество', 'Россия и бывший СССР', 'Общество', 'Общество', 'Россия и бывший СССР', 'Спорт и здоровье', 'Общество', 'Россия и бывший СССР', 'Медиа и СМИ', 'Общество', 'Технологии', 'Общество', 
'Россия и бывший СССР', 'Россия и бывший СССР', 'Медиа и СМИ', 'Россия и бывший СССР', 'Россия и бывший СССР']

df['tags'] = np.select(conditions, choices, default='Разное')

conditions1 = [(df['tags'] == 'Политика'), (df['tags'] == 'Общество'), (df['tags'] == 'Экономика'), (df['tags'] == 'Медиа и СМИ'), (df['tags'] == 'Технологии'), (df['tags'] == 'Спорт и здоровье'), (df['tags'] == 'Россия и бывший СССР'),
(df['tags'] == 'Спорт и здоровье')]
choices1 = [1,2,3,4,5,6,7,8]
df['category_id'] = np.select(conditions1, choices1, default=0)


client = clickhouse_connect.get_client(host='localhost', username='default', password='')
connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')

df1 = df[df['published'] > time]
ph.to_clickhouse(df1, 'lentaRSS', index=False, chunksize=100000, connection=connection)
client.command('OPTIMIZE TABLE lentaRSS FINAL DEDUPLICATE')

print(df1)