import pandas as pd
import requests
import logging
import config
from io import StringIO

# Extract
def extract_from_csv(file_path: str):
    return pd.read_csv(file_path)

def extract_from_online(url: str):
    # Perform the request with authentication
    response = requests.get(url, auth=(config.USER, config.PASS))
    # Check if the request was successful
    if response.status_code == 200:
    # Use StringIO to read the string content
        return pd.read_csv(StringIO(response.text))
    else:
        logging.error(f"Pipeline failed: {response.status_code}")
