# %%
import os
import re
import sys
import time
import pathlib
import requests
import pandas as pd
from bs4 import BeautifulSoup

#%% Pathing
CURR_FILE = pathlib.Path(__file__).resolve()
PROJECT_DIR = CURR_FILE.parents[2]

#%% System Config
sys.path.append(str(CURR_FILE.parents[2]))

# %% Internal Modules
from src.utils.status_helper import displayText

# %%

# get the "LOCAL SABAH NEWS" section title for each page
def getTitle(location:str, page:str, url="https://www.dailyexpress.com.my") -> list:
    time.sleep(2)
    # creating extracting list
    local_url = url + '/local'
    location_url = local_url + f"/?location={location}"
    page_url = location_url + f'&pageNo={page}'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
        "Accept-Encoding": "*",
        "Connection": "keep-alive"
    }

    try:
        # request web
        r = requests.get(page_url, headers=headers, timeout=5)
        # to beautiful soup
        s = BeautifulSoup(r.text, 'html.parser')
    except:
        fail_to_scrap = {
            'location': location,
            'page': page
        }
        return fail_to_scrap

    # selecting "LOCAL SABAH NEWS" section
    contents = s.find_all('div', class_='content')

    # catch none errors
    if len(contents) == 0:
        return None

    newslist = []

    # extracting information
    for content in contents:
        # get title
        title = content.find('div', class_='title')
        
        # get date
        date = content.find('div', class_='newstag')

        news = {
            'date': date.text.split('\n')[1].strip(),
            'location': date.text.split('\n')[2].strip(),
            'title': title.text.strip(),
            'title_link': title.find('a')['href']
        }

        # if news is valid : to avoid getting nans
        if news:
            newslist.append(news)

    # status response
    displayText(
        f"Scraping {location} on page {page}"
    )
        
    return newslist

def getFailScrape(df, url="https://www.dailyexpress.com.my"):
    # create empty dataframe
    dft = pd.DataFrame() # store data
    rerun = pd.DataFrame() # to be run again
    
    # df will contain location & page
    for idx in df.index:
        text = f"Scraping {df['location'][idx]} on page {df['page'][idx]}"
        displayText(text)

        titles = getTitle(
            location=df['location'][idx],
            page=str(df['page'][idx])
        )

        time.sleep(2)

        if isinstance(titles, dict):
            text = f"Scraping {df['location'][idx]} on page {df['page'][idx]}"
            displayText(text)
            rerun = pd.concat([rerun, pd.DataFrame(titles, index=[0])])
        else:
            dft = pd.concat([dft, pd.DataFrame(titles)])

    return dft, rerun

def getLocations(URL="https://www.dailyexpress.com.my/index.cfm") -> list:
    locations = []
    
    r = requests.get(URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    # Find all <a> tags with href attribute containing the keyword 'local/?location'
    targets = soup.find_all('a', href=lambda href: href and 'local/?location' in href)

    # append all location to list
    for location in targets:
        textLocation = location.text.strip()
        textLocation = re.sub(r'\s', '%20', textLocation)
        locations.append(textLocation)

    return locations

# %%
baseURL = "https://www.dailyexpress.com.my"

if __name__ == "__main__":
    # scraping example
    titles = getTitle(
        location = "Kota%20Kinabalu",
        page = "1",
        url = baseURL
    )

# %%
