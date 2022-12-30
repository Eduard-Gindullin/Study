# Imports
#import sqlite3
import requests
import feedparser
import pandahouse as ph
import random
import clickhouse_connect
import pandas as pd
import numpy as np


# DB




d = feedparser.parse('https://www.vedomosti.ru/rss/news')
d['feed']['title']
data_list = []

for i in d["entries"]:
     data_list.append([i["title"],i["link"],i["tags"],i["published"]])
      
for j in d["entries"][i].tags[j].term:
        data_list.append([j["term"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])
 
print(df)