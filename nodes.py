import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
import os

# major versions to include
currentver = '1.7'
nextvers = '1.8'


url = "https://nodes.jholdstock.uk/user_agents"
data = requests.get(url).text
soup = BeautifulSoup(data, 'html.parser')
table = soup.find('table', class_='table-striped')

# Defining of the dataframe
df = pd.DataFrame(columns=['rank', 'useragent', 'count'])

# Collecting Ddata
for row in table.tbody.find_all('tr'):
    # Find all data for each column
    columns = row.find_all('td')
    if (columns != []):
        rank = columns[0].text.strip()
        useragent = columns[1].text.strip()
        count = columns[2].text.strip()
        df = pd.concat([df, pd.DataFrame.from_records([{'rank': rank, 'useragent': useragent, 'count': count}])])

df = df.set_index('rank')
# get today's date for file path
todayStr = str(date.today())
yearMonthStr = date.today().strftime("%Y/%m/")

# path for raw data
pathStr = './data/nodes-raw/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'
# save to csv
df.to_csv(filename)

# format the user agent
df['useragent'] = df['useragent'].apply(lambda st: st[20:len(st)-1])
df['useragent'] = df['useragent'].str.replace('pre','dev builds')
df['useragent'] = df['useragent'].str.replace('(',' ')
df['useragent'] = df['useragent'].str.replace(')','')

# replace older versions with other
df['useragent'] = df['useragent'].replace({'1.6.*': 'other', '1.5.*': 'other', '1.4.*': 'other'}, regex=True)
df['count'] = df['count'].astype(int)
# group others
df = df.groupby("useragent").agg({'count':'sum'})

# path for processed data
pathStr = './data/nodes/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'
# save to csv
df.to_csv(filename)
