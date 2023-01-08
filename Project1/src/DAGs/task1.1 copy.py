import feedparser
import pandahouse as ph
import clickhouse_connect
import pandas as pd
import numpy as np



pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)

d = feedparser.parse('https://www.vedomosti.ru/rss/news')
data_list = []
for i in d['entries']:
    data_list.append([i["title"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])
df['category'] = np.where(((df['tags']=='Экономика') | (df['tags']=='Бизнес') 
                          | (df['tags']=='Финансы') | (df['tags']=='Бизнес / Транспорт')),
                           'Экономика и бизнес', df['tags'])
df['category'] = np.where(((df['tags']=='Моя страна') | (df['tags']=='Москва')
                           | (df['tags']=='Город') | (df['tags']=='Кавказ')
                           | (df['tags']=='Среда обитания')),'Россия', df['tags'])

df['category'] = np.where(((df['tags']=='Международная панорама') | (df['tags']=='Бывший СССР')),
                          'Политика', df['tags'])

df['category'] = np.where(((df['tags']=='Силовые структуры') | (df['tags']=='Космос')),
                          'Армия и ОПК', df['tags']) 

df['category'] = np.where((df['tags']=='Технологии') | (df['tags']=='Космос'),'Наука и техника', df['tags'])

df['category'] = np.where(((df['tags']=='Новости партнеров') | (df['tags']=='Власть') | (df['tags']=='Политика / Власть') | (df['tags']=='Международные отношения')),'Политика', df['tags']) 

df['category'] = np.where(((df['tags']=='Из жизни') 
                           | (df['tags']=='Биографии и справки') | (df['tags']=='Особое мнение')),
                          'Происшествия', df['tags'])

df['category'] = np.where((df['tags']=='Туризм и отдых') | (df['tags']=='Туризм'),
                          'Путешествия', df['tags']) 

df['category'] = np.where((df['tags']=='Афиша Plus'),'Культура', df['tags'])

df['category'] = np.where((df['tags']=='Забота о себе'),'Здоровье', df['tags'])

df['category'] = np.where((df['tags']=='Ценности'),'Интернет и СМИ', df['tags'])

df['category'] = np.where(((df['tags']=='Авто') 
                           | (df['tags']=='Недвижимость')),'Недвижимость и Авто', df['tags'])


client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS vedomosti1 (title String, link String,tags String, category String, published String) ENGINE MergeTree ORDER BY published')

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')

ph.to_clickhouse(df, 'vedomosti1', index=False, chunksize=100000, connection=connection)
#df2 = client.command('Select published from vedomosti AS str formatDateTime(now(), '%Y, %d/%m %H:%M (:%S)')

#new = str(df["tags"]) Convert
#new = df["tags"].split(" ", n = 2, expand = True)
#link = d.find(attrs={"tags":"term"})
#data_list2 = []
#for j in d['entries{tags}']:
#    data_list2.append([j["term"],j["scheme"]])
#print(d.entries[0:200].tags[0].term)
print(df)