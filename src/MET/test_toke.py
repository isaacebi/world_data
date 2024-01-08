import os
import pathlib
import requests
import sqlite3
import pandas as pd


print(requests.__version__)

TOKEN = os.environ['MET_TOKEN']

print(len(TOKEN))

headers = {"Authorization": TOKEN}
params = {
    "datasetid": "FORECAST",
    "datacategoryid": "GENERAL",
    "locationid": "LOCATION:237",
    "start_date": "2024-01-01",
    "end_date": "2024-01-01",
}

URL = f'https://api.met.gov.my/v2.1/data?datasetid={params["datasetid"]}&datacategoryid={params["datacategoryid"]}&locationid={params["locationid"]}&start_date={params["start_date"]}&end_date={params["end_date"]}'

response = requests.get(url=URL, headers=headers)

print(response.status_code)


# pathing
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[1]
DATA_DIR = os.path.join(PROJECT_DIR, 'data')
DATA_RAW = os.path.join(DATA_DIR, 'raw', 'MET')

# DB
GENERAL_DATA = os.path.join(DATA_RAW, 'general.db')

#
def getDB(DB_Path):
    # create connection
    conn = sqlite3.connect(DB_Path)

    # query to pandas on forecast table
    df = pd.read_sql_query("SELECT * FROM forecast", conn)

    return df

print(getDB(GENERAL_DATA))
