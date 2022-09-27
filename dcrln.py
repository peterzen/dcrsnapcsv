import requests
import pandas as pd
from datetime import date
import os

pd.set_option('display.max_columns', None)

url = 'https://ln-map.jholdstock.uk//api/graph'
# get api response
response = requests.get(url)
strResposne = response.text
# get today's date for file path
todayStr = str(date.today())
yearMonthStr = date.today().strftime("%Y/%m/")
# path for data
pathStr = './data/lnd-raw/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.txt'
# save raw json response to a csv
with open(filename, "w", encoding="utf-8") as f:
    f.write(strResposne)