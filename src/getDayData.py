# %% Libraries
import os
import time
import pathlib
import sqlite3
import getDataMET
import pandas as pd
from datetime import date, datetime, timedelta

# %%
# PATH
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[1]
DATA_DIR = os.path.join(PROJECT_DIR, 'data')
TOKEN_PATH = os.path.join(DATA_DIR, 'MET', 'token.txt')
DATA_RAW = os.path.join(DATA_DIR, 'raw')

# DB
GENERAL_DATA = os.path.join(DATA_RAW, 'general.db')

# URL
MET_URL = "https://api.met.gov.my/v2"
MET_URL_DATA = MET_URL + "/data"

# %%
# TOKEN = getDataMET.read_text_file(TOKEN_PATH)
TOKEN = os.environ['MET_TOKEN']

# %%
def getDB(DB_Path):
    # create connection
    conn = sqlite3.connect(DB_Path)

    # query to pandas on forecast table
    df = pd.read_sql_query("SELECT * FROM forecast", conn)

    return df

def getDate(df):
    # start date 
    start_date = datetime.strptime("2023-12-01", '%Y-%m-%d').date()

    # get today date
    today = date.today()

    # db latest date
    latest_date = max(pd.to_datetime(df['date']).dt.date)

    # db oldest date
    old_date = min(pd.to_datetime(df['date']).dt.date)

    # sanity check
    len_expected = len(pd.date_range(start_date, today, freq='d'))
    len_actual = len(pd.date_range(old_date, latest_date + timedelta(days=1), freq='d'))

    # create a list of date range
    if old_date < start_date:
        sdate = start_date
        edate = today

    elif len_expected != len_actual:
        sdate = start_date
        edate = today
        
    else:
        sdate = latest_date
        edate = today

    # ranges to be extract
    date_list = pd.date_range(sdate, edate, freq='d')

    # existing list
    exist_list = pd.to_datetime(df['date']).dt.date.to_list()

    # date of list
    date_list = [x for x in date_list if x not in exist_list]

    return date_list

def getGenMET(date_list, location, URL=MET_URL_DATA, token=TOKEN):
    df = pd.DataFrame()

    print(token)

    # Throttling
    # Burst rate : 10 per minute
    # Sustained rate: 2000 per day

    # only do 10 iter per day
    if len(date_list) > 10:
        date_list = date_list[:10]

    for t in date_list:
        # get dates to string
        extract_date = t.strftime('%Y-%m-%d')

        # extract information
        getJSON = getDataMET.get_met_api_data(
        "FORECAST", location, extract_date, extract_date, URL, token
        )

        # delay
        time.sleep(10)

        # json to pandas
        jdf = getDataMET.extract_weather_info(getJSON)

        # concat pandas
        df = pd.concat([df, jdf], ignore_index=True)

    return df

def commitDB(df, gen_path):
    conn = sqlite3.connect(gen_path)

    # create sql if sql not exist
    create_sql = "CREATE TABLE IF NOT EXISTS forecast (data_type INTEGER, location_name TEXT, date TEXT, FGA TEXT, FGM TEXT, FGN TEXT, FMAXT INTEGER, FMINT INTEGER, FSIGW text)"
    cursor = conn.cursor()
    cursor.execute(create_sql)

    # insert new data to db
    for row in df.itertuples():
        insert_sql = f"INSERT INTO forecast (data_type, location_name, date, FGA, FGM, FGN, FMAXT, FMINT, FSIGW) \
            VALUES ({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}', {row[6]}, {row[7]}, '{row[8]}')"
        cursor.execute(insert_sql)

    # commit to db
    conn.commit()

# %%
if __name__ == "__main__":
    # get current db
    df_db = getDB(GENERAL_DATA)

    # get dates need to be request
    dates = getDate(df_db)

    # request information from MET - SABAH
    extract_df = getGenMET(dates, location='LOCATION:13')

    # to db
    commitDB(extract_df, GENERAL_DATA)
