# %%
import os
import json
import pathlib
import requests
import pandas as pd
from bs4 import BeautifulSoup

# %%
baseURL = "https://www.dailyexpress.com.my"

# %%
# get the "LOCAL SABAH NEWS" section title for each page
def getTitle(location:str, page:str, url="https://www.dailyexpress.com.my") -> list:
    # creating extracting list
    local_url = url + '/local'
    location_url = local_url + f"/?location={location}"
    page_url = location_url + f'&pageNo={page}'

    # request web
    r = requests.get(page_url)
    # to beautiful soup
    s = BeautifulSoup(r.text, 'html.parser')

    # selecting "LOCAL SABAH NEWS" section
    contents = s.find_all('div', class_='content')

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
            'title': title.text.strip()
        }

        # if news is valid : to avoid getting nans
        if news:
            newslist.append(news)
        
    return newslist

# %%
titles = getTitle(
    location = "Kota%20Kinabalu",
    page = "1",
    url = baseURL
)

# %%
