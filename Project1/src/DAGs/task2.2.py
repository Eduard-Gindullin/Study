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
d = feedparser.parse('https://www.vedomosti.ru/rss/news')
data_list = []
for i in d['entries']:
    data_list.append([i["title"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])
df1 = df[df['published'] < time]
ph.to_clickhouse(df1, 'vedomosti', index=False, chunksize=100000, connection=connection)

#df1 = df[df['title'] != published()]
#ph.to_clickhouse(df, 'vedomosti', index=False, chunksize=100000, connection=connection)

print(df1)