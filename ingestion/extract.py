import time
import os
import boto3
import pandas as pd
import requests
import tenacity
from datetime import datetime 
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

DEFAULT_CATEGORY_ID = 8322
DEFAULT_NUMBER_PAGES = 10

