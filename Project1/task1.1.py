# Imports
#import sqlite3
import requests
import feedparser
import os
import urllib
import random
import clickhouse_connect
import pandas as pd
import numpy as np


# Feeds
myfeeds = [
  'https://www.vedomosti.ru/rss/news',
]

# User agents
uags = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]

# Random User Agent (from uags list)
ua = random.choice(uags)

# Header
headers = {
  "Connection" : "close",  # another way to cover tracks
  "User-Agent" : ua
}

# Proxies
proxies = {
}

# DB


client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE IF NOT EXISTS vedomosti (title String, link String,tags String, published String) ENGINE MergeTree ORDER BY title')
#scriptDir = os.path.dirname(os.path.realpath(__file__))
#db_connection = sqlite3.connect(scriptDir + '/rss.sqlite')
#db = client.cursor()
#db.execute('CREATE TABLE IF NOT EXISTS vedomosti (title TEXT, date TEXT)')

d = feedparser.parse('https://www.vedomosti.ru/rss/news')
d['feed']['title']
data_list = []

for i in d["entries"]:
    data_list.append([i["title"],i["link"],i["tags"],i["published"]])
df = pd.DataFrame(data_list, columns=["title","link","tags","published"])
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
client.command(INSERT INTO vedomosti VALUES, df)
print(df)




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

