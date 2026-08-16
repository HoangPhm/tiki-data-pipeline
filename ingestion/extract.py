import time
import os
import boto3
import requests
import random 
import json 
from datetime import datetime 
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, retry_if_exception_type

load_dotenv()

# tiki book category id
DEFAULT_CATEGORY_ID = 8322
# default number of pages 
DEFAULT_NUMBER_PAGES = 10

#AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
#AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')
TIKI_BASE_URL = "https://api.tiki.vn/v2/products?"


@retry(stop=stop_after_attempt(3), 
       retry=retry_if_exception_type(requests.exceptions.RequestException),
       reraise=True
)
def fetch_page(category_id, page, headers):
    api_url = (f"{TIKI_BASE_URL}"
               f"limit=40&category={category_id}&page={page}")
    response = requests.get(api_url, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json().get('data', [])


def fetch_products(category_id=DEFAULT_CATEGORY_ID, num_pages=DEFAULT_NUMBER_PAGES):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    all_products = []

    for page in range(1, num_pages+1):
        print(f"Fetching page {page} from tiki API")

        try:
            data = fetch_page(category_id, page, headers)
        except requests.RequestException as e:
            print(f"request failed at page {page}: {e}")
            break

        if not data:
            print(f"No more data at page {page}. Stopping")
            break

        all_products.extend(data)
        print(f"Fetched {len(data)} products from page {page}.")

        time.sleep(random.uniform(.5, 2))

    return all_products


def save_to_s3(data, category_id):
    if not data:
        print('No data to save')
        return False

    # S3 doesn't recognize python obj
    # convert data to str
    json_string = json.dumps(data, indent=2)

    # upload time
    date = datetime.now().strftime("%Y%m%d_%H:%M:%S")

    s3_client = boto3.client('s3', region_name=AWS_DEFAULT_REGION)
    s3_key = f"bronze/tiki/{category_id}/{date}.json"

    try:
        s3_client.put_object(Body=json_string.encode('utf-8'), Bucket=S3_BUCKET, Key=s3_key)
    except ClientError as e:
        print(f'Unexpected S3 error occured: {e}')
        return False
    else:
        return True


if __name__ == '__main__':
    data = fetch_products()
    success = save_to_s3(data, DEFAULT_CATEGORY_ID)
    print('Upload succeeded' if success else 'Upload failed')
