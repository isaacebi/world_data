# %% Libraries
import os
import sys
import json
import time
import random
import pathlib
import sqlite3
import requests
import threading
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# %% PATHING
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[2]

DATA_DIR = os.path.join(PROJECT_DIR, 'data')
DATA_RAW = os.path.join(DATA_DIR, 'raw')
DATA_DE = os.path.join(DATA_RAW, 'DE')
TRACK_PATH = os.path.join(DATA_DE, 'static.json')

# DB
TITLE_PATH = os.path.join(DATA_DE, 'news.db')

# append system path
sys.path.append(str(CURR_FILE.parents[2]))

# %% Internal Module
import getData
from src.utils.status_helper import displayText
from src.utils import db_helper

# %%
# read json file
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

# create request connection
def getRequest(URL):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
        "Accept-Encoding": "*",
        "Connection": "keep-alive"
    }

    r = requests.get(URL, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    targets = soup.find_all('i')

    return targets

# will only valid if pageNumber is outer from the news
def binary_search(location:str, pageList:list) -> int:
    # first number, mid number, last number
    fNum = pageList[0]
    mNum = pageList[1]
    lNum = pageList[2]

    pageNum = pageList[2]

    fml_indicator = []

    for num in pageList:
        URL = f"https://www.dailyexpress.com.my/local/?location={location}&pageNo={num}"
        targets = getRequest(URL)
        indicator = any("Will be available soon..." in target.text for target in targets)
        fml_indicator.append(indicator)
    
    # for troubleshooting
    # print(pageList)
    # print(fml_indicator)

    # check end number
    URL = f"https://www.dailyexpress.com.my/local/?location={location}&pageNo={pageNum}"
    targets = getRequest(URL)
    indicator = any("Will be available soon..." in target.text for target in targets)

    print(f"Check {location} on page {pageNum}")

    # case 1 - current page is invalid and current - 1 page is valid
    LP_url = f"https://www.dailyexpress.com.my/local/?location={location}&pageNo={pageNum-1}"
    LP_target = getRequest(LP_url)
    LP_indicator = any("Will be available soon..." in target.text for target in LP_target)

    # case 2 - current page is valid and current + 1 page is invalid
    plusOneUrl = f"https://www.dailyexpress.com.my/local/?location={location}&pageNo={pageNum+1}"
    plusOneTarget = getRequest(plusOneUrl)
    plusOneIndicator = any("Will be available soon..." in target.text for target in plusOneTarget)

    # case 1
    if not LP_indicator and indicator:
        return pageList[2]-1
    
    # case 2
    elif plusOneIndicator and not indicator:
        return pageList[2]+1

    # decision for binary search
    if fml_indicator == [False, False, True]:
        mid_last_range = lNum - mNum
        pageNum = mNum + (mid_last_range // 2)
        pageList[2] = pageNum - 1
        return binary_search(location, pageList)
    
    elif fml_indicator == [False, True, True]:
        pageList[2] = mNum - 1
        pageList[1] = pageList[2] // 2
        return binary_search(location, pageList)
    
    elif fml_indicator == [False, False, False]:
        mid_last_range = lNum - mNum
        pageList[2] = lNum + mid_last_range
        return binary_search(location, pageList)

    else:
        return binary_search(location, pageList)

# get latest page
def getLastestPage(page:str, location='Kota%20Kinabalu'):

    if page == 0:
        # initial state
        edge_indicator = True
        page = 50

        while edge_indicator:
            baseURL = f"https://www.dailyexpress.com.my/local/?location={location}&pageNo={page}"
            targets = getRequest(baseURL)

            for target in targets:
                # check if latest page
                if "Will be available soon..." in target.text:
                    # binary search
                    print("Initiate Binary Search")
                    fNum = page // 2
                    mNum = fNum + fNum // 2
                    lNum = page
                    pageList = [fNum, mNum, lNum]

                    # get the actual page
                    page = binary_search(location, pageList)
                    edge_indicator = False # breaking

            if edge_indicator:
                page += 50

    while True:
        indicator = False

        text = f"Latest page from {location} is {page}"
        displayText(text)

        baseURL = f"https://www.dailyexpress.com.my/local/?location={location}&pageNo={page}"
        targets = getRequest(baseURL)

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
if __name__ == "__main__":
    #################
    # Initilization #
    #################
    today_page = {}
    FORCE_INITIAL_STATE = True

    # create empty dataframe
    dft = pd.DataFrame()
    rerun = pd.DataFrame()

    # threading
    executor = ThreadPoolExecutor(16)
    thred_lock = threading.Lock()

    ######################
    # Get Previous State #
    ######################

    # create db connection
    news_db = db_helper.SQLite_Helper(
        DB_path=TITLE_PATH
    )

    # get existing db
    df_title = news_db.getDB(
        tableName='title_new'
    )

    # get recent page
    yesterday_page = getJson(TRACK_PATH)

    # get all locations
    locations = getData.getLocations()

    # removing duplicated if any
    locations = list(dict.fromkeys(locations))


    #
    # Operation
    #

    # go through location by location and get the data
    # make it go through one by one, its not ideal to run the whole batch in a single run
    # to consider using threading, but ip block is a problem
    for loc in locations:

        # update today page - work with empty file
        try:
            # to force start from empy
            if FORCE_INITIAL_STATE == True:
                yesterday_page[loc] = 0
                
            today_page[loc] = getLastestPage(
                page=yesterday_page[loc],
                location=loc
            )
            # get new page to be extract
            new_page = today_page[loc] - yesterday_page[loc]

        except:
            today_page[loc] = getLastestPage(
                page=0,
                location=loc
            )
            new_page = 0

        futures = []
        # only scrape the new page title
        for p in range(1, today_page[loc]):
            future = executor.submit(getData.getTitle, loc, str(p))
            futures.append(future)

        for future in futures:
            titles = future.result()

            if isinstance(titles, dict):
                # status response
                displayText(
                    f"Fail to scrape {loc} on page {p}"
                )
                rerun = pd.concat([rerun, pd.DataFrame(titles, index=[0])])
            else:
                # status response
                displayText(
                    f"Merging variable"
                )
                with thred_lock:
                    dft = pd.concat([dft, pd.DataFrame(titles)])

        # success scenario
        dft = pd.concat([dft, df_title]).drop_duplicates(keep=False)

        # commit in every location
        # information scraped
        information_values = {
            'TEXT': ['date', 'location', 'title', 'title_link']
        }

        # commit db
        news_db.commitDB(
            tableName='title_new',
            values=information_values,
            df = dft
        )

        # fail scenario
        failed_information = {
            'TEXT': ['location'],
            'INTEGER': ['page']
        }
        # commitDBF(rerun, 'fail_title', conn)
        news_db.commitDB(
            tableName='fail_title',
            values=failed_information,
            df=rerun
        )

    # update json file
    with open(TRACK_PATH, 'w') as f:
        json.dump(today_page, f)
