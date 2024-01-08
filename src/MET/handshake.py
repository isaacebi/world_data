# %% Libraries
import os
import pathlib
import requests
import sqlite3
import pandas as pd

# internal module
import getDataMET

# check request version
requestVer = requests.__version__
text = f'The libraries are {requestVer}'
cmd = "echo {}".format(text)
os.system(cmd)

# %% 
# PATHING
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[2]
DATA_DIR = os.path.join(PROJECT_DIR, 'data')
DATA_RAW = os.path.join(DATA_DIR, 'raw', 'MET')
TOKEN_PATH = os.path.join(DATA_DIR, 'MET', 'token.txt')

# DB
GENERAL_DATA = os.path.join(DATA_RAW, 'general.db')

# %% Helper Function
def getDB(DB_Path):
    # create connection
    conn = sqlite3.connect(DB_Path)

    # query to pandas on forecast table
    df = pd.read_sql_query("SELECT * FROM forecast", conn)

    return df

# to get token based on state run which either local or gitaction
def getToken(token=TOKEN_PATH):
    # check if in gitaction
    if os.getenv("GITHUB_ACTIONS") :
        TOKEN = os.environ['MET_TOKEN']
        print("Proceed with secret token")
        return TOKEN
    else :
        TOKEN = getDataMET.read_text_file(token)
        print("Proceed with local token")
        return TOKEN
        

# %%
if __name__ == "__main__":

    TOKEN = getToken(token=TOKEN_PATH)

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

    text = f'The response code are {response.status_code}'
    cmd = "echo {}".format(text)
    os.system(cmd)