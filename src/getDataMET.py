# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 11:19:39 2023

@author: isaac
"""

# %% IMPORTING
import os
import sys
import pathlib
import requests
import pandas as pd

# %% Solving sys
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[1]

# adding path
sys.path.append(str(PROJECT_DIR))

# own module
from src import getInfoMET

# %% PATH
# current folder
current_folder = os.getcwd()
# main folder
folder = os.path.dirname(current_folder)
# data folder
data = os.path.join(folder, 'data')
# data > MET folder
data_met = os.path.join(data, 'MET')
# token
token_MET = os.path.join(data, 'MET', 'token.txt')


# URL
MET_URL = "https://api.met.gov.my/v2"
MET_URL2 = "https://api.met.gov.my/v2/data"
DATA_TYPE_URL = "https://api.met.gov.my/v2/datatypes"
LOCATION_BASE_URL = "https://api.met.gov.my/v2.1/locations?locationcategoryid="

# %% STATIC
LOCATIONS = ['STATE', 'DISTRICT', 'TOWN', 'TOURISTDEST', 'WATERS']


# %%

def read_text_file(file_path):
    '''
    To get the token value - access point for MET

    Parameters
    ----------
    file_path : str
        Token file path

    Returns
    -------
    str
        Unique MET token.

    '''
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            # return in the correct MET token acceptance
            return f"METToken {content}"
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None


def get_met_api_data(datasetid, locationid, start_date, end_date, URL, TOKEN):
    url = URL
    headers = {"Authorization": TOKEN}

    params = {
        "datasetid": datasetid,
        "datacategoryid": "GENERAL",
        "locationid": locationid,
        "start_date": start_date,
        "end_date": end_date,
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        # Process the API response data here
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


def extract_weather_info(api_response):
    weather_info_list = []

    for result in api_response['results']:
        location_name = result['locationname']
        date = result['date']
        data_type = result['datatype']
        value = result['value']

        weather_info = {
            'location_name': location_name,
            'date': date,
            'data_type': data_type,
            'value': value
        }

        # list of data
        weather_info_list.append(weather_info)
        # convert into DataFrame
        weather_df = pd.DataFrame(weather_info_list)
        # pivot dataframe
        pivot_df = weather_df.pivot(index=['location_name', 'date'], 
                                    columns='data_type', 
                                    values='value').reset_index()
    # return as pivot tables
    return pivot_df


# %%
# create token
# TOKEN = read_text_file(token_MET)

# ---------------------------------------- #
# Extract general information from MET API #
# ---------------------------------------- #
# uncomment if need the csv back
# getInfoMET.getCSV(DATA_TYPE_URL, LOCATION_BASE_URL, LOCATIONS, data_met, TOKEN)


# %%

# Example usage
# start_date = "2023-12-01"
# end_date = "2023-12-01"

# result = get_met_api_data("FORECAST", start_date, end_date, MET_URL2, TOKEN)
    
# weather_data = extract_weather_info(result)
