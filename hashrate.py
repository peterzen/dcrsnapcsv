import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
import os
import lxml

url = "https://poolbay.io/crypto/54/decred"
data = requests.get(url).text
soup = BeautifulSoup(data, 'html.parser')

# Parse the html content
soup = BeautifulSoup(data, "lxml")

gdp_table = soup.find("table", attrs={"id": "pool-table"})

name = soup.find_all('td', class_='td-name')

table = soup.find( "table", {"id":"pool-table"} )

# list for pool names
listPool=[]
# populate pool names
for row in table.find_all('td', class_='td-name'):
    vpool = row.find('a').contents[0]
    listPool.append(vpool)

# list for hashrate and units
listHash=[]
listUnits=[]

# populate hashrate and units
for span in table.find_all('span', class_= 'mt-2'):
    if span.text != "":
        hashrate = span.text
        rate = float(hashrate[:(hashrate.find('.')+3)])
        units = hashrate[(hashrate.find('.')+3):]
        # convert to Ph/s
        if units == 'TH/s':
            rate = rate / 1000
            units = 'PH/s'
        if units == 'H/s':
            rate = rate / 10e15
            units = 'PH/s'
        vpool = row.find('a').contents[0]
        listHash.append(rate)
        listUnits.append(units)

# create dict
d = { 'pool':listPool, 'rate':listHash,'units':listUnits}
# create df
df = pd.DataFrame(d)

# check if path exists, if not create
if not os.path.exists('./data/hashrate'):
    os.makedirs('./data/hashrate')

# get today's date to add to the filename
todayStr = str(date.today())
filename = './data/hashrate/hashrate-' + todayStr + '.csv'

# save to csv
df.to_csv(filename)