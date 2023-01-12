import pandahouse as ph
import feedparser
import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(host='localhost', username='default', password='')

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')
last_published = client.command('Select * from lenta where published IN (SELECT MAX(`published`) as `time` FROM `lenta`)')
time = last_published[3]
d = feedparser.parse('https://lenta.ru/rss/')
data_list = []
for i in d['entries']:
    data_list.append([i["summary"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["summary","link","tags","published"])
df1 = df[df['published'] > time]
#ph.to_clickhouse(df1, 'lenta', index=False, chunksize=100000, connection=connection)
client.command('OPTIMIZE TABLE lenta FINAL DEDUPLICATE')

print(df1)