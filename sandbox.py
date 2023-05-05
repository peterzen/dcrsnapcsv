import pandas as pd
from datetime import date
import datetime as dt
import requests
import os
import stream
import time
import utils.cm

pd.set_option('display.max_columns', None)

# specify base path string
basePathStr = './data/dex_decred_org'

# check if path exists, if not create
if not os.path.exists(basePathStr):
    os.makedirs(basePathStr)

# get today's date for file path
todayStr = str(date.today())
yearMonthStr = date.today().strftime("%Y/%m/")

# grab server configuration
configUrl = "https://dex.decred.org/api/config"
response = requests.get(configUrl)
data = response.json()
# extract asset list
assets = pd.json_normalize(data['assets']).set_index('id')
# extract markets list
markets = pd.DataFrame.from_dict(data['markets'])
# replace base with strings
markets['base'] = markets['base'].apply(lambda x: assets.loc[x].symbol)
# replace quote with strings
markets['quote'] = markets['quote'].apply(lambda x: assets.loc[x].symbol)
# path for saving the data
pathStr = basePathStr + '/markets/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'
# save to csv
markets.to_csv(filename,index=False)


for index, row in assets.iterrows():
    # check if the symbol includes the base network (e.g. .eth)
    if "." in row.symbol:
        xSymbol = row.symbol.split(".", 1)[0]
    else:
        xSymbol = row.symbol
    dYday = pd.to_datetime(dt.date.today() - dt.timedelta(days=1), utc=True, format='%Y-%m-%dT%H:%M:%S', errors='ignore')
    PriceUSD = utils.cm.getMetric(xSymbol,'PriceUSD',dYday,dYday)
    assets.at[index, 'PriceUSD'] = PriceUSD['PriceUSD'][0]
