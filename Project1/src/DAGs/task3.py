import pandahouse as ph
import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(host='localhost', username='default', password='')

connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')
client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS all news (title String, link String,tags String, published String) ENGINE MergeTree ORDER BY published')
client.command('CREATE TABLE IF NOT EXISTS all news (title String, link String,tags String, published String) ENGINE MergeTree ORDER BY published')

published = []
published.append(client.command('Select DISTINCT title, published from vedomosti'))
data_list2 = []
for j in published:
  data_list2.append([j["title"],j["published"]])
df1 = pd.DataFrame(data_list2, columns=['title',"published"])