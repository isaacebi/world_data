import os
import pathlib
import requests

print(requests.__version__)

TOKEN = os.environ['MET_TOKEN']

headers = {"Authorization": TOKEN}
params = {
    "datasetid": "FORECAST",
    "datacategoryid": "GENERAL",
    "locationid": "LOCATION:237",
    "start_date": "2024-01-01",
    "end_date": "2024-01-01",
}

URL = f'https://api.met.gov.my/v2.1/data?datasetid={params["datasetid"]}&datacategoryid={params["datacategoryid"]}&locationid={params["locationid"]}&start_date={params["start_date"]}&end_date={params["end_date"]}'

# response = requests.get(url=URL, headers=headers)

# print(response.status_code)
