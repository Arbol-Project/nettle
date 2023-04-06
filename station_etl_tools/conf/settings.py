import os, sys
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
S3_STATION_BUCKET = os.environ.get("S3_STATION_BUCKET")
GATEWAY_URL = os.environ.get("GATEWAY_URL")

# paths relative to the script directory
# LOCAL_INPUT_ROOT = os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), "datasets")
# OUTPUT_ROOT = os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), "climate")
LOCAL_INPUT_ROOT = os.path.join(os.getcwd(), "datasets")
OUTPUT_ROOT = os.path.join(os.getcwd(), "climate")
HASHES_OUTPUT_ROOT = os.path.join(OUTPUT_ROOT, "hashes")

# this will ensure cfgrib can find libeccodes
os.environ["ECCODES_DIR"] = os.path.join(os.path.dirname(__file__), "../eccodes/")

# Print user folder
# os.getcwd()
# os.path.abspath('')
# from pathlib import Path
# Path.cwd()