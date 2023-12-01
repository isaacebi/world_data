# %%
from datetime import datetime, timedelta
import time
import requests
import pandas as pd
import numpy as np
import os

# %%
TOKEN_PATH = os.path.join(os.getcwd(), 'token', 'token.txt')
DATA_PATH = os.path.join(os.getcwd(), 'data', 'earthquake.csv')

# %%
df_main = pd.DataFrame()

# %%
with open(TOKEN_PATH) as f:
    rapid_token = f.readline()

# %%
def jsonToDataframe(jsonVar):
    df = pd.DataFrame()

    for i in range(len(jsonVar)):
        df_json = pd.DataFrame([jsonVar[i]])
        df = pd.concat([df, df_json], ignore_index=True)

    return df

def getRapidApi(start_date, end_date, rapid_token):
    url = "https://everyearthquake.p.rapidapi.com/earthquakesByDate"

    querystring = {
        "startDate":start_date,
        "endDate":end_date,
        "start":"1",
        "count":"100",
        "type":"earthquake",
        "latitude":"5.420404",
        "longitude":"116.796783",
        "radius":"1000",
        "units":"miles",
        "magnitude":"2.5",
        "intensity":"1"
    }

    headers = {
        "X-RapidAPI-Key": rapid_token,
        "X-RapidAPI-Host": "everyearthquake.p.rapidapi.com"
    }

    response = requests.request(
        "GET", 
        url, 
        headers=headers, 
        params=querystring
    )
    time.sleep(10) # avoid getting 429
    return response

# %%
# date initialisation
startDate = datetime(1990, 1, 1)
endDate = datetime(2020, 1, 1)

# stores 31 days that can be added
addDays = timedelta(days=365)

while startDate <= endDate:
    # extract data
    response = getRapidApi(
        start_date=startDate, 
        end_date=startDate+addDays, 
        rapid_token=rapid_token
    )

    # into dataframe
    results = response.json()['data']

    try:
        dfTemporary = jsonToDataframe(results)
        df_main = pd.concat([df_main, dfTemporary], ignore_index=True)

    except KeyError:
        raise KeyError("Check Json File from Rapid API")    

    # add a month - to end iteration
    startDate += addDays

# %%
df_main.drop(columns=['locationDetails'], inplace=True)
df_main.to_csv(DATA_PATH, index=False)

# %%
