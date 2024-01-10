# %% Libraries
import os
import json
import pathlib
import requests
import pandas as pd
from bs4 import BeautifulSoup

# %% PATHING
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[2]

DATA_DIR = os.path.join(PROJECT_DIR, 'data')
DATA_RAW = os.path.join(DATA_DIR, 'raw')
DATA_DE = os.path.join(DATA_RAW, 'DE')
TRACK_PATH = os.path.join(DATA_DE, 'static.json')


# %%
def getJson(jsonPath):
    # read json file
    with open(jsonPath, 'r') as f:
        fileData = f.read()
        jsonData = json.loads(fileData)

        # initialization
        trackData = {}

        # json to dict
        for i in jsonData:
            trackData[i] = jsonData[i]

    return trackData

def getLastestPage(page:str, location='Kota%20Kinabalu'):
    
    while True:
        indicator = False

        baseURL = f"https://www.dailyexpress.com.my/local/?location={location}&pageNo={page}"
        r = requests.get(baseURL)
        soup = BeautifulSoup(r.text, 'html.parser')

        targets = soup.find_all('i')
        for target in targets:
            # check if latest page
            if "Will be available soon..." in target.text:
                indicator = True

        if not indicator:
            page += 1
        else:
            break

    return page


# %%
yesterday_page = getJson(TRACK_PATH)
today_page = getLastestPage(page=yesterday_page['no_page'])

scrape_page = today_page - yesterday_page


# %%
