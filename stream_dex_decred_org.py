import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import os

# this script iterates through the data points stored
# as daily csv files in this repo to combine them
# into a single data stream file with multiple columns

# set boundaries to iterate
dateStart = dt.date(int(2023),int(5),int(1))
dateEnd = dt.date.today()
# file path
sPath = 'data/stream/dex_decred_org_VolUSDT.csv'
# remove existing file
if os.path.isfile(sPath):
    os.remove(sPath)
# initialise pointer
datex = dateStart
# iterate over the dates covered
while datex <= dateEnd:
    # generate strings
    dateStr = str(datex)
    yearMonthStr = datex.strftime("%Y/%m/")
    fPath = './data/dex_decred_org/spots/' + yearMonthStr + dateStr + '.csv'
    # check if data point file exists
    if os.path.isfile(fPath):
        # read data point file into df
        df = pd.read_csv(fPath)
        df['market'] = df['baseID'] + "_" + df['quoteID']
        # copy only needed columns
        rawdf = df[['market', 'vol24USDT']].copy()
        # set pool as index
        rawdf = rawdf.set_index('market')
        # transpose
        sData = rawdf.T
        # add date to df
        sData['date'] = dateStr
        # set date as index
        sData = sData.set_index('date')
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
            fDataNew = pd.concat([fData,sData], axis=0, ignore_index=False)
            # fill NAs with 0 since each sample wont have all column values
            fDataNew = fDataNew.fillna(0)
            # fix column ordering
            fDataNew = fDataNew.reindex(sorted(fDataNew.columns), axis=1)
            # overwrite the file
            fDataNew.to_csv(sPath, mode='w', header=True)
    else:
        print('No data found for '+dateStr)
    datex += dt.timedelta(days=1)
print(fDataNew)