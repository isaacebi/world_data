# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 14:26:28 2023

@author: isaac
"""

# %%
import os
import time
import requests
import pandas as pd

# %%

def read_text_file(file_path):
    '''
    To get the token value

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

class InfoAPI:
    def __init__(self, url, token):
        self.url = url
        self.headers = {"Authorization": token}

    def extract_data_types(self):
        '''
        To extract data types from MET API

        Returns
        -------
        DataFrame
            Description of data types from MET API.

        '''
        response = requests.get(self.url, headers=self.headers)

        if response.status_code == 200:
            api_response = response.json()
            
            # Process the API response data here
            # ------------------------------------------------------- #
            # *** If the structure JSON changes, change this code *** #
            # ------------------------------------------------------- #
            data_types_list = []
            
            for result in api_response['results']:
                data_type_id = result['id']
                data_type_name = result['name']
                dataset_id = result['datasetid']
                data_category_id = result['datacategoryid']

                data_type_info = {
                    'data_type_id': data_type_id,
                    'data_type_name': data_type_name,
                    'dataset_id': dataset_id,
                    'data_category_id': data_category_id
                }

                data_types_list.append(data_type_info)
                
            return pd.DataFrame(data_types_list)

        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
        
    def extract_location(self):
        response = requests.get(self.url, headers=self.headers)

        if response.status_code == 200:
            api_response = response.json()
            
            # Process the API response data here
            # ------------------------------------------------------- #
            # *** If the structure JSON changes, change this code *** #
            # ------------------------------------------------------- #
            location_info_list = []
        
            for result in api_response['results']:
                location_id = result['id']
                location_name = result['name']
                latitude = result['latitude']
                longitude = result['longitude']
        
                location_info = {
                    'location_id': location_id,
                    'location_name': location_name,
                    'latitude': latitude,
                    'longitude': longitude
                }
        
                location_info_list.append(location_info)
                
            return pd.DataFrame(location_info_list)

        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

# this work fine, but only get the initial 50 data.   
def getCSV(urlDataTypes, urlBaseLoc, loc_list, saveFolder, TOKEN):

    # gets data types
    result = InfoAPI(urlDataTypes, TOKEN).extract_data_types()
    path_save = os.path.join(saveFolder, "datatypes.csv")
    result.to_csv(path_save, index=False)

    # location information
    for loc in loc_list:
        loc_url = urlBaseLoc + loc
        result = InfoAPI(loc_url, TOKEN).extract_location()
        path_save = os.path.join(saveFolder, f"{loc}.csv")
        result.to_csv(path_save, index=False)

    print("Completed getting all MET general API information")

def getDisCSV(TOKEN, savePath, urlDis='https://api.met.gov.my/v2.1/locations?locationcategoryid=DISTRICT'):
    offSetNum = 0
    df = pd.DataFrame()

    while True:
        URL = urlDis + "&offset=" + str(offSetNum)
        results = InfoAPI(URL, TOKEN).extract_location()
        df = pd.concat([df, results], ignore_index=True)
        path_save = os.path.join(savePath, "DISTRICT.csv")
        df.to_csv(path_save, index=False)

        if results is not None:
            time.sleep(10)
            offSetNum += 50

        else:
            break

    return df


# %%
