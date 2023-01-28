import os
import boto3
from urllib.parse import urlparse
from datetime import datetime


s3 = boto3.client("s3")

files = [
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/acord125_pages_1-4_<TIMESTAMP>.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/acord126_pages_5-8_<TIMESTAMP>.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/acord140_pages_9-11_<TIMESTAMP>.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/property_affidavit_pages_12-12_<TIMESTAMP>.csv"
  }
]

folder_name = "csvfiles_" + datetime.utcnow().isoformat()
os.mkdir(folder_name)
for file in files:
  s3_bucket = urlparse(file['JoinedCSVOutputPath']).netloc
  s3_prefix = urlparse(file['JoinedCSVOutputPath']).path.lstrip('/')
  s3_filename, _ = os.path.splitext(os.path.basename(file['JoinedCSVOutputPath']))
  local_name = f"{folder_name}/{s3_filename}.csv"
  s3.download_file(s3_bucket, s3_prefix, local_name)
