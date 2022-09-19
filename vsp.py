import requests
import pandas as pd
from datetime import date
import os

pd.set_option('display.max_columns', None)

url = 'https://api.decred.org/?c=vsp'
# get api response
response = requests.get(url)
data = response.json()
# convert to dataframe
df = pd.read_json(url)
# transpose
dft = df.T
# set the index label as id
dft.index.name = 'id'
# convert unix time to something readable
dft['launched'] = pd.to_datetime(dft['launched'],unit='s')
dft['lastupdated'] = pd.to_datetime(dft['lastupdated'],unit='s')

# check if path exists, if not create
if not os.path.exists('./data/vsp'):
    os.makedirs('./data/vsp')

# get today's date for file path
todayStr = str(date.today())
yearMonthStr = date.today().strftime("%Y/%m/")

# path for raw data
pathStr = './data/vsp/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'

# save to csv
dft.to_csv(filename)