import requests
import pandas as pd
from datetime import date
import os
import json
from urllib.request import urlopen
import numpy as np
from csv import writer

def appendtoCSV(data,path,filename):
    # this function takes data as a list and appends to the csv file in the specified path
    if not os.path.exists(path):
        # create path if it doesn't exist
        os.makedirs(path)
    filePath = path + filename
    with open(filePath,  'a', newline='') as f_object:
        # Pass this file object to csv.writer()
        # and get a writer object
        writer_object = writer(f_object)
        # Pass the list as an argument into
        writer_object.writerow(data)
        # Close the file object
        f_object.close()
