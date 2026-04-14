import os
from dotenv import load_dotenv

# Load variables from .env into environment
load_dotenv()

# Read values
INPUT_FILE = os.getenv("INPUT_FILE")
REF_FILE = os.getenv("REF_FILE")
BASE_OUTPUT = os.getenv("BASE_OUTPUT", "data_output")

DB_URL = os.getenv("DB_URL")
TABLE_NAME = os.getenv("TABLE_NAME")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
