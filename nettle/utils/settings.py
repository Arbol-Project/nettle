import os
import sys
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

RAW_DATA_ROOT = os.path.join(os.getcwd(), "raw_data")
PROCESSED_DATA_ROOT = os.path.join(os.getcwd(), "processed_data")
HASHES_OUTPUT_ROOT = os.path.join(PROCESSED_DATA_ROOT, "hashes")

# Env is dev, prod
APP_MODE = os.environ.get('APP_MODE', 'dev')
