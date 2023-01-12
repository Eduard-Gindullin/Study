import pandahouse as ph
import clickhouse_connect
import pandas as pd
import numpy as np

client = clickhouse_connect.get_client(host='localhost', username='default', password='')

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')
client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS all_news (title String, link String,tags String, published String) ENGINE MergeTree ORDER BY published')
client.command('CREATE TABLE IF NOT EXISTS all_categories (category_id Int32, category_name String, tags String) ENGINE MergeTree ORDER BY category_id')

data_list1 = client.command('Select DISTINCT tags from vedomosti')
data_list2 = client.command('Select DISTINCT tags from tass')
data_list3 = client.command('Select DISTINCT tags from lenta')
data_list4 = data_list1 + '\n' +  data_list2 + '\n' + data_list3
#df = pd.read_table(data_list4)
df = pd.DataFrame([x.split(';') for x in data_list4.split('\n')])
df = df.rename(columns={0:'tags'})


conditions = [(df['tags'] == 'Политика') , (df['tags'] == 'Общество'), (df['tags'] == 'Бизнес'), (df['tags'] == 'Экономика'), (df['tags'] == 'Финансы'), (df['tags'] == 'Медиа'), (df['tags'] == 'Авто'), (df['tags'] == 'Политика / Власть'), 
(df['tags'] == 'Политика / Международные отношения'), (df['tags'] == 'Технологии'), (df['tags'] == 'Среда обитания'), (df['tags'] == 'Недвижимость'), (df['tags'] == 'Экономика и бизнес'), (df['tags'] == 'Армия и ОПК'), (df['tags'] == 'Происшествия'),
(df['tags'] == 'Культура'), (df['tags'] == 'НедвижимостьОбщество'), (df['tags'] == 'Международная панорама'), (df['tags'] == 'Спорт'), (df['tags'] == 'Москва'), (df['tags'] == 'Северный Кавказ'), (df['tags'] == 'Космос'), (df['tags'] == 'Биографии и справки'),
(df['tags'] == 'Военная операция на Украине'), (df['tags'] == 'Мир'), (df['tags'] == 'Забота о себе'), (df['tags'] == 'Россия'), (df['tags'] == 'Путешествия'), (df['tags'] == 'Ценности'), (df['tags'] == 'Бывший СССР'), (df['tags'] == 'Интернет и СМИ'),
(df['tags'] == 'Силовые структуры'), (df['tags'] == 'Наука и техника'), (df['tags'] == 'Из жизни'), (df['tags'] == 'Моя страна')]

choices = ['Политика', 'Общество', 'Экономика', 'Экономика', 'Экономика', 'Медиа и СМИ', 'Технологии', 'Политика', 'Политика', 'Технологии', 'Общество', 'Экономика', 'Экономика', 'Технологии', 'Общество', 'Общество', 'Экономика', 'Политика', 'Спорт и здоровье', 
'Россия и бывший СССР', 'Россия и бывший СССР', 'Технологии', 'Общество', 'Россия и бывший СССР', 'Общество', 'Общество', 'Россия и бывший СССР', 'Спорт и здоровье', 'Общество', 'Россия и бывший СССР', 'Медиа и СМИ', 'Общество', 'Технологии', 'Общество', 'Россия и бывший СССР']

df['category'] = np.select(conditions, choices, default='Разное')

conditions1 = [(df['category'] == 'Политика'), (df['category'] == 'Общество'), (df['category'] == 'Экономика'), (df['category'] == 'Медиа и СМИ'), (df['category'] == 'Технологии'), (df['category'] == 'Спорт и здоровье'), (df['category'] == 'Россия и бывший СССР'),
(df['category'] == 'Спорт и здоровье')]
choices1 = [1,2,3,4,5,6,7,8]
df['category_id'] = np.select(conditions1, choices1, default=0)

#df['category'] = np.where(((df['category']=='Экономика') | (df['category']=='Бизнес') 
#                           | (df['category']=='Финансы') | (df['category']=='Бизнес / Транспорт')),
#                          'Экономика и бизнес', df['category'])

#df['category','category_id'] = np.where()
#df.index.names = ['Category_id']


#pd.DataFrame(data_list1, columns=["category_id", "category_name", "tags"])

#client.command('INSERT INTO `default`.all_categories (category_name, tags) VALUES(sport String, sport String)')
#client.command('ALTER TABLE default.all_categories UPDATE (category_id, category_name) VALUES("sport","sport")')

#client.query('select tags from all_categories where tags = "Политика"')
#client.command('INSERT INTO default.all_categories (category_id, category_name) VALUES (1, "Общество") select category_id, Category_name, tags from all_categories WHERE tags IS Общество')
print(df)

""" published = []
published.append(client.command('Select DISTINCT title, published from vedomosti'))
data_list2 = []
for j in published:
  data_list2.append([j["title"],j["published"]])
df1 = pd.DataFrame(data_list2, columns=['title',"published"]) """