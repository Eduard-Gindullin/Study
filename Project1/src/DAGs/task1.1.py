import feedparser
import pandahouse as ph
import clickhouse_connect
import pandas as pd
import re



pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)

d = feedparser.parse('https://www.vedomosti.ru/rss/news')
data_list = []
for i in d['entries']:
    data_list.append([i["title"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])
df['published'] = df['published'].astype('datetime64[ns]')


client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS vedomosti_rss (title String, link String,tags String, published DateTime) ENGINE MergeTree ORDER BY published')


#def read_sql(vedomosti):
#	data, columns = client.command(vedomosti, columnar=True, with_column_types=True)
#df3 = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})

#df2 = pd.DataFrame(d, columns=["title","link","tags","published"])
d2 = client.command('SELECT*FROM vedomosti')
data_list2 = []
#for i in d2:
#   data_list2.append([i["title"],i["link"],i["tags"],i["published"]])
#df2 = pd.DataFrame({'col':d2})
df2 = pd.DataFrame(d2, columns=["title","link","tags","published"])
#df2 = pd.DataFrame([x.split(';') for x in df1.split('\n')])

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')

#ph.to_clickhouse(df, 'vedomosti_rss', index=False, chunksize=100000, connection=connection)
#df2 = client.command('Select published from vedomosti AS str formatDateTime(now(), '%Y, %d/%m %H:%M (:%S)')

#new = str(df["tags"]) Convert
#new = df["tags"].split(" ", n = 2, expand = True)
#link = d.find(attrs={"tags":"term"})
#data_list2 = []
#for j in d['entries{tags}']:
#    data_list2.append([j["term"],j["scheme"]])
#print(d.entries[0:200].tags[0].term)
print(df2)