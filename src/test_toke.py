import os
import pathlib
import requests

TOKEN = os.environ['MET_TOKEN']

URL = "https://api.met.gov.my/v2/data"
headers = {"Authorization": TOKEN}
params = {
    "datasetid": "FORECAST",
    "datacategoryid": "GENERAL",
    "locationid": "LOCATION:237",
    "start_date": "2024-01-01",
    "end_date": "2024-01-01",
}

response = requests.get(url=URL, headers=headers, params=params)

print(response.status_code)
