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


client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS vedomosti (title String, link String,tags String, published String) ENGINE MergeTree ORDER BY published')


d = feedparser.parse('https://www.vedomosti.ru/rss/news')

data_list = []

for i in d["entries"]:
  
    data_list.append([i["title"],i["link"],i[d['entries'][i].tags[j].term],i["published"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])
connection = dict(database='default',
                  host='http://localhost:8123',
                  user='default',
                  password='')

ph.to_clickhouse(df, 'vedomosti', index=False, chunksize=100000, connection=connection)
#df['tags'] = list(df['tags'])
#df['category'] = []
#a = df['tags']
#if a == [{'term': 'Политика', 'scheme': None, 'label': None}]:
#  df["category"].append('Политика')
#for i, row in enumerate(df["tags"]):
# df["category"].append(df["tags"][i][1])
#for i in df.tags:
#df['tags'] = df['tags']['term']
#df['tags'] = df.tags.str.replace("'[{'term': '","', 'scheme': None, 'label'" , '')
#df['tags'].replace("[{'term': 'Политика', 'scheme': None, 'label': None}]", 'Политика')
#client.insert('vedomosti', data_list, column_names=["title","link","tags","published"]) 
#client.command('INSERT INTO vedomosti VALUES, df')
#print(df)




# Get posts from DB and print
#def get_posts():
#    with client:
#        print(client.command("SELECT * FROM vedomosti"))

#article_title = 'privet'
#article_date = '12.12'   

# Check post in DB
#def article_is_not_db(article_title, article_date):
#    client.command("SELECT * from vedomosti WHERE title=? AND date=?", (article_title, article_date))
 #   if not db.fetchall():
 #       return True
  #  else:
   #     return False

# Add post to DB
#def add_article_to_db(article_title, article_date):
#    client.command("INSERT INTO vedomosti VALUES (?,?)", (article_title, article_date))
#    client.execute()

