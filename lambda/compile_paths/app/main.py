# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import boto3

logger = logging.getLogger(__name__)

s3 = boto3.resource("s3")


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'DEBUG')

    logger.setLevel(log_level)
    logger.info(json.dumps(event))

    documents = list()
    if len(event):
        i = 0
        while i < len(event):
            document_type = event[i][str(i + 1)]
            start_page = i + 1
            document = dict()
            document['document_type'] = document_type
            document['output_csv_paths'] = list()
            document['table_csv_paths'] = dict()
            while event[i][str(i + 1)] == document_type:
                document['output_csv_paths'].append(event[i]['TextractOutputCSVPath'])
                tables_pair = event[i]['TextractOutputTablesPaths']
                document['table_csv_paths'][tables_pair[0]] = tables_pair[1]
                i += 1
                if i == len(event):
                    break
            document['original_document_pages'] = f"{start_page}-{i}"
            documents.append(document)
    else:
        logger.warning("no items found in event")

    logger.debug(json.dumps(documents))
    return documents