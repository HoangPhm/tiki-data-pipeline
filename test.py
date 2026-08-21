import boto3

S3_BUCKET = 'tiki-data-bucket'

# dùng đúng key đã confirm có thật trong S3
s3_key = "bronze/tiki/2026-08-21_16-09-02/8322.json"

s3_client = boto3.client('s3')

try:
    response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
    print(f"Found: s3://{S3_BUCKET}/{s3_key}")
    print(f"Size: {response['ContentLength']} bytes")
except s3_client.exceptions.ClientError as e:
    print(f"Not found: {e}")