# Imports
import feedparser
import pandahouse as ph
import clickhouse_connect
import pandas as pd
import numpy as np

d = feedparser.parse('https://lenta.ru/rss/')
data_list = []
for i in d['entries']:
    data_list.append([i["summary"],i["link"],i["tags"][0].term,i["published"]])
df = pd.DataFrame(data_list, columns=["summary","link","tags","published"])
df['published'] = df['published'].astype('datetime64[ns]')
df = df.rename(columns={'summary': 'title'})

print(df['published'].dt.day_name())
