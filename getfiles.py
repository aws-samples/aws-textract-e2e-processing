import os
import boto3
from urllib.parse import urlparse
from datetime import datetime


s3 = boto3.client("s3")

files = [
{
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/claimform_pages_1-1_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "claimform_page1": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/claimform_page1/table_1.csv"
      ],
    }
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/dischargesummary_pages_2-2_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "dischargesummary_page1": []
    }
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/doctorsnote_pages_3-3_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "doctorsnote_page1": []
    }
  }
]


main_csvfiles_folder = "csvfiles"

if not os.path.exists(main_csvfiles_folder):
  os.mkdir(main_csvfiles_folder)

folder_name = f"{main_csvfiles_folder}/csvfiles_{datetime.utcnow().isoformat()}"
os.mkdir(folder_name)
os.mkdir(f"{folder_name}/tables")
for file in files:
  s3_bucket = urlparse(file['JoinedCSVOutputPath']).netloc
  s3_prefix = urlparse(file['JoinedCSVOutputPath']).path.lstrip('/')
  s3_filename, _ = os.path.splitext(os.path.basename(file['JoinedCSVOutputPath']))
  local_name = f"{folder_name}/{s3_filename}.csv"
  print(local_name)
  s3.download_file(s3_bucket, s3_prefix, local_name)

  table_outputs = file["TextractOutputTablesPaths"]
  for document_with_page_num, table_files in table_outputs.items():
    os.mkdir(f"{folder_name}/tables/{document_with_page_num}")
    for j, table_file in enumerate(table_files):
      s3_bucket = urlparse(table_file).netloc
      s3_prefix = urlparse(table_file).path.lstrip('/')
      s3_filename, _ = os.path.splitext(os.path.basename(table_file))
      local_name = f"{folder_name}/tables/{document_with_page_num}/{s3_filename}.csv"
      print(f"\t{local_name}")
      s3.download_file(s3_bucket, s3_prefix, local_name)
