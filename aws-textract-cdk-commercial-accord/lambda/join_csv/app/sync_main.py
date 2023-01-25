# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import boto3
import csv

import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime

logger = logging.getLogger(__name__)
s3 = boto3.client('s3')


def split_s3_path_to_bucket_and_key(s3_path):
    if len(s3_path) > 7 and s3_path.lower().startswith("s3://"):
        s3_bucket, s3_key = s3_path.replace("s3://", "").split("/", 1)
        return s3_bucket, s3_key
    else:
        raise ValueError(
            f"s3_path: {s3_path} is no s3_path in the form of s3://bucket/key."
        )


def get_file_from_s3(s3_path):
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(s3_path)
    o = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    return o.get('Body').read()


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'DEBUG')
    logger.setLevel(log_level)
    logger.info(json.dumps(event))
    logger.info(f"boto3 version: {boto3.__version__}.")

    s3_output_bucket = os.environ.get('JOINED_S3_OUTPUT_BUCKET')
    s3_output_prefix = os.environ.get('JOINED_S3_OUTPUT_PREFIX')

    if not s3_output_bucket or not s3_output_prefix:
        raise ValueError(
            f"no s3_output_bucket: {s3_output_bucket} or s3_output_prefix: {s3_output_prefix} defined."
        )
    logger.debug(f"LOG_LEVEL: {log_level} \n \
                    S3_OUTPUT_BUCKET: {s3_output_bucket} \n \
                    S3_OUTPUT_PREFIX: {s3_output_prefix}")

    all_df = []
    static_cols = ["Timestamp", "Classification", "Base Filename", "Feature Type",
                    "Alias", "Value"]
    for s3_path in event['output_csv_paths']:
        file_bytes = get_file_from_s3(s3_path)
        with BytesIO(file_bytes) as f:
            reader = csv.reader(StringIO(f.getvalue().decode('UTF-8')))
            num_cols = 0
            for row in reader:
                num_cols = max(num_cols, len(row))
            column_names = list(range(num_cols))
            df = pd.read_csv(f, names=column_names)
            all_df.append(df)

    result = pd.concat(all_df)
    column_names = static_cols + list(range(len(result.columns) - 6))
    result.columns = column_names
            
    result_bytes = result.to_csv(index=False)
    s3_filename = f"{event['document_type']}_pages_{event['original_document_pages']}"
    
    output_bucket_key = s3_output_prefix + "/" + s3_filename + "_" + datetime.utcnow(
    ).isoformat() + ".csv"
    logger.debug(s3_output_bucket)
    logger.debug(s3_output_prefix)
    logger.debug(output_bucket_key)
    
    s3.put_object(Body=result_bytes,
                  Bucket=s3_output_bucket,
                  Key=output_bucket_key)
    
    return {
        "JoinedCSVOutputPath":
        f"s3://{s3_output_bucket}/{output_bucket_key}"
    }
