import pandas as pd
from datetime import date
import os

# major versions to include
import stream

currentver = '1.7'
nextvers = '1.8'


url = "https://nodes.jholdstock.uk/api/user_agents"
df = pd.read_json(url).set_index('rank')

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
df['count'] = df['count'].astype(int)
rawdf = df[['useragent', 'count']].copy()

# replace older versions with other
df['useragent'] = df['useragent'].replace({'1.6.*': 'other', '1.5.*': 'other', '1.4.*': 'other'}, regex=True)

# group others
df = df.groupby("useragent").agg({'count':'sum'})
# path for processed data
pathStr = './data/nodes/' + yearMonthStr
if not os.path.exists(pathStr):
    os.makedirs(pathStr)
filename = pathStr + todayStr + '.csv'
# save to csv
df.to_csv(filename)

# append total node count to the nodes stream
# check if the file exists:
streamPath = './data/stream/'
streamFile = 'countNodes.csv'

# check if file path exists:
if not os.path.isfile((streamPath+streamFile)):
    # if it doesn't exist, create header and file
    stream.appendtoCSV(['date','countNodes'],streamPath,streamFile)
# create data list
data = [todayStr,df['count'].sum()]
# update stream file
stream.appendtoCSV(data,streamPath,streamFile)

# this bit of code is for appending to the nodes by ver stream
# set useragent as index
rawdf = rawdf.set_index('useragent')
# transpose
sData = rawdf.T
# add date to df
sData['date'] = todayStr
# set date as index
sData = sData.set_index('date')
# file path
sPath = 'data/stream/countNodesByVer.csv'
# check if stream file exists
if not os.path.isfile(sPath):
    # if it doesn't exist, create file with header
    sData.to_csv(sPath, mode='w', header=True)
else:
    # if the file does exist
    # read stream file into df
    fData = pd.read_csv(sPath)
    # set index
    fData = fData.set_index('date')
    # concat both dataframes
    fDataNew = pd.concat([fData, sData], axis=0, ignore_index=False)
    # fill NAs with 0 since each sample wont have all column values
    fDataNew = fDataNew.fillna(0)
    # convert all columns to int
    fDataNew = fDataNew.astype(int)
    # fix column ordering
    fDataNew = fDataNew.reindex(sorted(fDataNew.columns), axis=1)
    # overwrite the file
    fDataNew.to_csv(sPath, mode='w', header=True)

# this bit of code is for appending to the nodes by ver stream
# transpose
sData = rawdf.T
# add date to df
sData['date'] = todayStr
# set date as index
sData = sData.set_index('date')
# file path
sPath = 'data/stream/countNodesByVer.csv'
# check if stream file exists
if not os.path.isfile(sPath):
    # if it doesn't exist, create file with header
    sData.to_csv(sPath, mode='w', header=True)
else:
    # if the file does exist
    # read stream file into df
    fData = pd.read_csv(sPath)
    # set index
    fData = fData.set_index('date')
    # concat both dataframes
    fDataNew = pd.concat([fData, sData], axis=0, ignore_index=False)
    # fill NAs with 0 since each sample wont have all column values
    fDataNew = fDataNew.fillna(0)
    # convert all columns to int
    fDataNew = fDataNew.astype(int)
    # fix column ordering
    fDataNew = fDataNew.reindex(sorted(fDataNew.columns), axis=1)
    # overwrite the file
    fDataNew.to_csv(sPath, mode='w', header=True)