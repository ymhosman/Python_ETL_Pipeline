import pandas as pd
# Extract
def extract_from_csv(file_path: str):
    return pd.read_csv(file_path)