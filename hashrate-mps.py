import requests
import pandas as pd
from datetime import date
from datetime import datetime
import json
import time
import os

dt = date.today()

dt = datetime.combine(dt, datetime.min.time())
time = dt.timestamp()
nowStr = str(int(time))

pd.set_option('display.max_columns', None)
#build url
url = 'https://miningpoolstats.stream/data/time?t=' + nowStr

# get api response
response = requests.get(url)
ts = response.json()
# set user agent
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
url = 'https://data.miningpoolstats.stream/data/decred.js?t=' + str(ts)
response = requests.get(url, headers=headers)
jsonResponse = response.json()
strResposne = response.text
# get today's date for file path
todayStr = str(date.today())
yearMonthStr = date.today().strftime("%Y/%m/")
# path for data
pathStr = './data/hashrate-mps-raw/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'
# save raw json response to a csv
with open(filename, "w") as f:
    f.write(strResposne)


# values to get from the data array
cols = ['url', 'hashrate', 'miners','minpay','id','pool_id','feetype','workers','income','i_blocks','blocks_nr','lastblock',
        'lastblocktime','blocks_1000','blocks_count','hashrate_average_100_count','hashrate_average_100'
        'hashrate_average_1h','hashrate_average_7d','hashrate_average_7d_count','expected_blocks','luck']
# create empty df
df = pd.DataFrame(columns=cols)

jsonData = jsonResponse['data']
#print('data')
# scan through the data array
for i in jsonData:
    # create empty dict
    dfDict = {}
    # get all parameters from the cols list
    for x in cols:
        if x in i:  # if the parameter exists add to the dict
            dfDict[x] = i[x]
        else:       # if it doesn't exist, add an 0
            dfDict[x] = 0
    # add the dict to the dataframe
    df = pd.concat([df, pd.DataFrame.from_records([dfDict])])
#set index
df = df.set_index('pool_id')

# path for data
pathStr = './data/hashrate-mps/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'
# save to csv
df.to_csv(filename)