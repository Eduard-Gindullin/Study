# Imports
import feedparser
import pandahouse as ph
import clickhouse_connect
import pandas as pd



pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)

d = feedparser.parse('https://tass.ru/rss/v2.xml')
data_list = []
for i in d["entries"]:
    data_list.append([i["title"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])
client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS tass (title String, link String,tags String, published String)ENGINE MergeTree ORDER BY published')

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')

ph.to_clickhouse(df, 'tass', index=False, chunksize=100000, connection=connection)

