# Imports
import sqlite3
import requests
#import feedparser
import os
import urllib
import random


# Feeds
myfeeds = [
  'https://lenta.ru/rss/',
]

import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default', password='')
client.command('CREATE TABLE lenta (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key')