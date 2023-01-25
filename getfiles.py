import os
import boto3
from urllib.parse import urlparse
from datetime import datetime


s3 = boto3.client("s3")

files = [
  {
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-70uw1q72o402/textract-joined-output/acord125_pages_1-4_2023-01-10T03:49:36.410738.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-70uw1q72o402/textract-joined-output/acord126_pages_5-8_2023-01-10T03:49:36.413892.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-70uw1q72o402/textract-joined-output/acord140_pages_9-11_2023-01-10T03:49:36.585691.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-70uw1q72o402/textract-joined-output/property_affidavit_pages_12-12_2023-01-10T03:49:36.265905.csv"
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
