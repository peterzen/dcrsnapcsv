import requests
import pandas as pd
from datetime import date
import os
import json
from urllib.request import urlopen
import numpy as np

pd.set_option('display.max_columns', None)

def convert_ln_json_to_df(jsonResponse):
    graph_json = (jsonResponse)

    df_nodes = pd.json_normalize(graph_json['nodes'])

    df_channels = pd.json_normalize(graph_json['edges'])
    df_channels.channel_id = df_channels.channel_id.astype(np.int64)
    df_channels.capacity = df_channels.capacity.astype(int)

    for index, row in df_nodes.iterrows():
        ipk = row['pub_key']
        node1 = df_channels[df_channels['node1_pub']==ipk]
        outCount =  node1.channel_id.count().astype(int)
        outCapacity = node1.capacity.sum().astype(int)
        node2 = df_channels[df_channels['node2_pub']==ipk]
        inCount =  node2.channel_id.count().astype(int)
        inCapacity = node2.capacity.sum().astype(int)
        df_nodes.loc[index, 'outCount'] = outCount
        df_nodes.loc[index, 'outCapacity'] = outCapacity
        df_nodes.loc[index, 'inCount'] = inCount
        df_nodes.loc[index, 'inCapacity'] = inCapacity
        df_nodes.loc[index, 'chCount'] = inCount+outCount

    return df_nodes, df_channels

# get today's date for file path
todayStr = str(date.today())
yearMonthStr = date.today().strftime("%Y/%m/")

url = 'https://raw.githubusercontent.com/bochinchero/dcrsnapcsv/main/data/dcrlndgraph/' + yearMonthStr + todayStr +".json"
# get api response
response = requests.get(url)
strResposne = response.text

response = urlopen(url)
data_json = json.loads(response.read())
df_nodes, df_channels = convert_ln_json_to_df(data_json)
df_nodes = df_nodes.sort_values(by='chCount', ascending=False)

pathStr = './data/lnd-nodes/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'
# save to csv
df_nodes.to_csv(filename)