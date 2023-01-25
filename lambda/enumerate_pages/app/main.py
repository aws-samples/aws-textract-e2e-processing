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
    logger.info("length of event: " + str(len(event)))
    #logger.info(json.dumps(event, indent=4))

    if len(event):
        # order by page number
        event.sort(key=lambda e: int(os.path.splitext(os.path.basename(e['manifest']['s3Path']))[0]))
        i = 0
        while i < len(event):
            counter = 1
            document_type = event[i]['classification']['documentType']
            while event[i]['classification']['documentType'] == document_type:
                event[i]['classification']['documentTypeWithPageNum'] = f"{document_type}_page{counter}"
                counter += 1
                i += 1
                if i == len(event):
                    break
    else:
        logger.warning("no items found in event")

    logger.info(json.dumps(event, indent=4))
    logger.info(len(event))
    return event


