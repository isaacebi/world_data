# %% Libraries
import os
import json
import time
import random
import pathlib
import sqlite3
import requests
import pandas as pd
from bs4 import BeautifulSoup

# internal module
import getData

# %% PATHING
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[2]

DATA_DIR = os.path.join(PROJECT_DIR, 'data')
DATA_RAW = os.path.join(DATA_DIR, 'raw')
DATA_DE = os.path.join(DATA_RAW, 'DE')
TRACK_PATH = os.path.join(DATA_DE, 'static.json')

# DB
TITLE_PATH = os.path.join(DATA_DE, 'news.db')

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
    print(pageList)
    print(fml_indicator)

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

# TODO: need a proper data structure - too long to scrape latest page
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
        getData.displayText(text)

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

def getDB(cnx, tableName='title') -> pd.DataFrame():
    # query to pandas on forecast table
    df = pd.read_sql_query(f"SELECT * FROM {tableName}", cnx)
    return df

def commitDB(df:pd.DataFrame(), tableName:str, cnx):
    # skip if df is not true
    if df.empty:
        text = "Nothing to commit"
        getData.displayText(text)
        return None

    # create sql if sql not exist
    create_sql = f"CREATE TABLE IF NOT EXISTS {tableName} (date TEXT, location TEXT, title TEXT)"
    cursor = cnx.cursor()
    cursor.execute(create_sql)

    # insert new data to db
    for row in df.itertuples():
        insert_sql = f"INSERT INTO {tableName} (date, location, title) VALUES (?, ?, ?)"
        data = (row[1], row[2], row[3])
        cursor.execute(insert_sql, data)

    # commit to db
    cnx.commit()

def commitDBF(df:pd.DataFrame(), tableName:str, cnx):
    # skip if df is not true
    if df.empty:
        text = "Nothing to commit"
        getData.displayText(text)
        return None

    # create sql if sql not exist
    create_sql = f"CREATE TABLE IF NOT EXISTS {tableName} (location TEXT, page INTEGER)"
    cursor = cnx.cursor()
    cursor.execute(create_sql)

    # insert new data to db
    for row in df.itertuples():
        insert_sql = f"INSERT INTO {tableName} (location, page) VALUES (?, ?)"
        data = (row[1], row[2])
        cursor.execute(insert_sql, data)

    # commit to db
    cnx.commit()

# %%
    
if __name__ == "__main__":
    today_page = {}

    # create db connection
    conn = sqlite3.connect(TITLE_PATH)
    text = "Connecting DB"
    getData.displayText(text)

    # get db
    df_title = getDB(conn) # success
    # df_fail = getDB(conn, 'fail_title')

    # get recent page
    yesterday_page = getJson(TRACK_PATH)

    # get all locations
    locations = getData.getLocations()
    # removing duplicated if any
    locations = list(dict.fromkeys(locations))

    # create empty dataframe
    dft = pd.DataFrame() # store data
    rerun = pd.DataFrame() # to be run again

    # go through location by location and get the data
    # make it go through one by one, its not ideal to run the whole batch in a single run
    for loc in locations:
        # if loc == 'Keningau' or loc == 'Kota%20Belud':
        #     continue

        # update today page - work with empty file
        try:
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

        # only scrape the new page title
        for p in range(1, today_page[loc]):
            text = f"Scraping {loc} on page {p}"
            getData.displayText(text)

            time.sleep(2)

            
            titles = getData.getTitle(
                location=loc,
                page=str(p)
            )

            if isinstance(titles, dict):
                text = f"Fail to scrape {loc} on page {p}"
                getData.displayText(text)
                rerun = pd.concat([rerun, pd.DataFrame(titles, index=[0])])
            else:
                dft = pd.concat([dft, pd.DataFrame(titles)])

        # success
        dft = pd.concat([dft, df_title]).drop_duplicates(keep=False)
        # commit in every location
        commitDB(dft, 'title', conn)

        # fail
        commitDBF(rerun, 'fail_title', conn)

    # update json file
    with open(TRACK_PATH, 'w') as f:
        json.dump(today_page, f)

# %%