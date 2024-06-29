# Imports
import os
import sys
import pathlib
import requests
import numpy as np
import pandas as pd
from PyPDF2 import PdfReader
from scidownl import scihub_download

# Pathing
## folder path
PROJECT_DIR = os.path.dirname(os.getcwd())
DATA_DIR = os.path.join(PROJECT_DIR, 'data')
PAPER_DIR = os.path.join(DATA_DIR, 'paper')

## file path
DOI_FILE = os.path.join(PAPER_DIR, 'doi_list.txt')
DOI_FAIL = os.path.join(PAPER_DIR, 'doi_fail.txt')

# main
# Initialize empty list to track doi
doi_list = []

# read text file that consist all doi
with open(DOI_FILE, 'r') as f:
    for line in f:
        doi_list.append(line.rstrip())

def checkFile(fullfile):
    with open(fullfile, 'rb') as f:
        try:
            pdf = PdfReader(f)
            info = pdf.metadata
            if info:
                return True
            else:
                return False
        except:
            return False