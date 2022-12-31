import feedparser
import pandahouse as ph
import clickhouse_connect
import pandas as pd



pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)

d = feedparser.parse('https://www.vedomosti.ru/rss/news')
data_list = []
for i in d['entries']:
    data_list.append([i["title"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])


client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS vedomosti (title String, link String,tags String, published String) ENGINE MergeTree ORDER BY published')

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')

ph.to_clickhouse(df, 'vedomosti', index=False, chunksize=100000, connection=connection)
#df2 = client.command('Select published from vedomosti AS str formatDateTime(now(), '%Y, %d/%m %H:%M (:%S)')

#new = str(df["tags"]) Convert
#new = df["tags"].split(" ", n = 2, expand = True)
#link = d.find(attrs={"tags":"term"})
#data_list2 = []
#for j in d['entries{tags}']:
#    data_list2.append([j["term"],j["scheme"]])
#print(d.entries[0:200].tags[0].term)
#print(df2)