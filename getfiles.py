import os
import boto3
from urllib.parse import urlparse
from datetime import datetime


s3 = boto3.client("s3")

files = [
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/acord125_pages_1-4_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "acord125_page1": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page1/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page1/table_2.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page1/table_3.csv"
      ],
      "acord125_page2": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page2/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page2/table_2.csv"
      ],
      "acord125_page3": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page3/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page3/table_2.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page3/table_3.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page3/table_4.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page3/table_5.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page3/table_6.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page3/table_7.csv"
      ],
      "acord125_page4": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page4/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord125_page4/table_2.csv"
      ]
    }
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/acord126_pages_5-8_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "acord126_page1": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page1/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page1/table_2.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page1/table_3.csv"
      ],
      "acord126_page2": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page2/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page2/table_2.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page2/table_3.csv"
      ],
      "acord126_page3": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page3/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page3/table_2.csv"
      ],
      "acord126_page4": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord126_page4/table_1.csv"
      ]
    }
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/acord140_pages_9-11_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "acord140_page1": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord140_page1/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord140_page1/table_2.csv"
      ],
      "acord140_page2": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord140_page2/table_1.csv",
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/acord140_page2/table_2.csv"
      ],
      "acord140_page3": []
    }
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/property_affidavit_pages_12-12_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "property_affidavit_page1": []
    }
  }
]

folder_name = "csvfiles/csvfiles_" + datetime.utcnow().isoformat()
os.mkdir(folder_name)
os.mkdir(f"{folder_name}/tables")
for file in files:
  s3_bucket = urlparse(file['JoinedCSVOutputPath']).netloc
  s3_prefix = urlparse(file['JoinedCSVOutputPath']).path.lstrip('/')
  s3_filename, _ = os.path.splitext(os.path.basename(file['JoinedCSVOutputPath']))
  local_name = f"{folder_name}/{s3_filename}.csv"
  s3.download_file(s3_bucket, s3_prefix, local_name)

  table_outputs = file["TextractOutputTablesPaths"]
  for document_with_page_num, table_files in table_outputs.items():
    os.mkdir(f"{folder_name}/tables/{document_with_page_num}")
    for j, table_file in enumerate(table_files):
      s3_bucket = urlparse(table_file).netloc
      s3_prefix = urlparse(table_file).path.lstrip('/')
      s3_filename, _ = os.path.splitext(os.path.basename(table_file))
      local_name = f"{folder_name}/tables/{document_with_page_num}/{s3_filename}.csv"
      print(local_name)
      s3.download_file(s3_bucket, s3_prefix, local_name)
