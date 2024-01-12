# %% Libraries
import os
import time
import random
import pathlib
import sqlite3
import pandas as pd
from datetime import date, datetime, timedelta

# internal module
import getDataMET

# %%
# PATH
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[2]
DATA_DIR = os.path.join(PROJECT_DIR, 'data')
DATA_SECRET = os.path.join(DATA_DIR, 'secret')
DATA_RAW = os.path.join(DATA_DIR, 'raw')
DATA_MET = os.path.join(DATA_RAW, 'MET')
TOKEN_PATH = os.path.join(DATA_SECRET, 'MET.txt')

# DB
GENERAL_DATA = os.path.join(DATA_MET, 'general.db')

# URL
MET_URL = "https://api.met.gov.my/v2"
MET_URL_DATA = MET_URL + "/data"

# %%
def displayText(texts):
    cmd = 'echo {}'.format(texts)
    os.system(cmd)

# to get token based on state run which either local or gitaction
def getToken(token=TOKEN_PATH):
    # check if in gitaction
    if os.getenv("GITHUB_ACTIONS") :
        TOKEN = os.environ['MET_TOKEN']
        
        # displaying status
        text = "Proceed with secret token"
        displayText(text)
        return TOKEN
    
    else :
        TOKEN = getDataMET.read_text_file(token)

        # displaying status
        text = "Proceed with local token"
        displayText(text)
        return TOKEN
        

def getDB(cnx) -> pd.DataFrame():
    # query to pandas on forecast table
    df = pd.read_sql_query("SELECT * FROM forecast", cnx)
    return df

# This function need to have test unit. Like really
def getDate(df) -> list:
    # start date 
    start_date = datetime.strptime("2019-01-01", '%Y-%m-%d').date()

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
    date_list = pd.date_range(sdate, edate, freq='d').strftime('%Y-%m-%d').to_list()

    # existing list
    exist_list = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d').to_list()

    # date of list
    date_list = [x for x in date_list if x not in exist_list]

    # the item return should be in list, and item in the list should be str
    return date_list

# TODO: check the dataframe output
def getGenMET(date_list, location, URL=MET_URL_DATA, token=getToken(), iter=5):
    df = pd.DataFrame()

    # Throttling
    # Burst rate : 10 per minute
    # Sustained rate: 2000 per day

    # only do 10 iter per day
    if len(date_list) > iter:
        date_list = date_list[:iter]

    for t in date_list:
        # check dtype, if not str then change to str
        if type(t) != str:
            extract_date = t.strftime('%Y-%m-%d')
        else:
            extract_date = t

        # extract information
        getJSON = getDataMET.get_met_api_data(
        "FORECAST", location, extract_date, extract_date, URL, token
        )

        # delay
        time.sleep(10)
        # display progress
        text = f'Currently extracting {t}'
        displayText(text)

        # sometimes, there are no data for the date, therefore this is to skip process if such cases exist
        if getJSON is not None:
            # json to pandas
            jdf = getDataMET.extract_weather_info(getJSON)

            # concat pandas
            df = pd.concat([df, jdf], ignore_index=True)

    return df

def commitDB(df:pd.DataFrame(), cnx):
    # skip if df is not true
    if df.empty:
        text = "Nothing to commit"
        displayText(text)
        return None

    # create sql if sql not exist
    create_sql = "CREATE TABLE IF NOT EXISTS forecast (data_type INTEGER, location_name TEXT, date TEXT, FGA TEXT, FGM TEXT, FGN TEXT, FMAXT INTEGER, FMINT INTEGER, FSIGW text)"
    cursor = cnx.cursor()
    cursor.execute(create_sql)

    # insert new data to db
    for row in df.itertuples():
        insert_sql = f"INSERT INTO forecast (data_type, location_name, date, FGA, FGM, FGN, FMAXT, FMINT, FSIGW) \
            VALUES ({row[0]}, '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}', {row[6]}, {row[7]}, '{row[8]}')"
        cursor.execute(insert_sql)

    # commit to db
    cnx.commit()

# %%
if __name__ == "__main__":
    # create db connection
    conn = sqlite3.connect(GENERAL_DATA)
    text = "Connecting DB"
    displayText(text)
    
    # get current db
    df_db = getDB(conn)

    # get dates need to be request
    dates = getDate(df_db.drop_duplicates())

    # get all location id for general forecast
    locations = getDataMET.getLocation(DATA_MET)

    # get random sampling - to avoid from overuse gitaction free time
    locations = random.sample(locations, 5)

    for loc in locations:
        text = f'Extracting from {loc}'
        displayText(text)

        # request information from MET - SABAH
        extract_df = getGenMET(dates, location=loc)

    # to db
    commitDB(extract_df, conn)

    # close db connection
    conn.cursor().close()
    text = "Closing DB Connection"
    displayText(text)
