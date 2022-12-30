import pandahouse as ph
import random
import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(host='localhost', username='default', password='')

print(client.command("SELECT * published FROM vedomosti"))

