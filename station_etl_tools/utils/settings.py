import os
import sys
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
S3_STATION_BUCKET = os.environ.get("S3_STATION_BUCKET")

LOCAL_INPUT_ROOT = os.path.join(os.getcwd(), "raw_data")
OUTPUT_ROOT = os.path.join(os.getcwd(), "processed_data")
HASHES_OUTPUT_ROOT = os.path.join(OUTPUT_ROOT, "hashes")
