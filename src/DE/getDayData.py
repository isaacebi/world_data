# %% Libraries
import os
import json
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

def getDB(cnx, tableName='title') -> pd.DataFrame():
    # query to pandas on forecast table
    df = pd.read_sql_query(f"SELECT * FROM {tableName}", cnx)
    return df

# TODO: need to create class to handle success db and fail
def commitDB(df:pd.DataFrame(), cnx):
    # skip if df is not true
    if df.empty:
        text = "Nothing to commit"
        getData.displayText(text)
        return None

    # create sql if sql not exist
    create_sql = "CREATE TABLE IF NOT EXISTS title (date TEXT, location TEXT, title TEXT, title_link TEXT)"
    cursor = cnx.cursor()
    cursor.execute(create_sql)

    # insert new data to db
    for row in df.itertuples():
        insert_sql = "INSERT INTO title (date, location, title, title_link) VALUES (?, ?, ?, ?)"
        data = (row[1], row[2], row[3], row[4])
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
    df_title = getDB(conn)

    # get recent page
    yesterday_page = getJson(TRACK_PATH)

    # get all locations
    locations = getData.getLocations()
    # removing duplicated if any
    locations = list(dict.fromkeys(locations))

    # create empty dataframe
    dft = pd.DataFrame()

    # go through location by location and get the data
    for loc in locations:
        # update today page
        today_page[loc] = getLastestPage(
            page=yesterday_page[loc],
            location=loc
        )

        # get new page to be extract
        new_page = today_page[loc] - yesterday_page[loc]

        # only scrape the new page title
        for p in range(1, new_page[loc]+1):
            titles = getData.getTitle(
                location=loc,
                page=str(p)
            )

            dft = pd.concat([dft, pd.DataFrame(titles)])

    dft = pd.concat([dft, df_title]).drop_duplicates(keep=False)

    # update json file
    with open(TRACK_PATH, 'w') as f:
        json.dump(today_page, f)

    # update database
    commitDB(pd.DataFrame(titles), conn)