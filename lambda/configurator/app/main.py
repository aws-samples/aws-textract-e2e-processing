# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import boto3
import json
import textractmanifest as tm

logger = logging.getLogger(__name__)
version = "0.0.13"

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    table_name = os.environ.get('CONFIGURATION_TABLE')

    logger.setLevel(log_level)
    logger.info(f"version: {version}")
    logger.info(f"schadem-tidp-manifest version: {tm.__version__}")
    logger.info(f"table_name: {table_name}")
    logger.info(json.dumps(event))
    if 'classification' in event:
        if 'documentTypeWithPageNum' in event['classification']\
                and 'documentType' in event['classification']:
            document_type_with_page_num = event['classification']['documentTypeWithPageNum']
            document_type = event['classification']['documentType']
            logger.debug(f"document_type_with_page_num: {document_type_with_page_num}\n" +
                         f"document_type: {document_type}")
        else:
            raise ValueError(
                'no [classification][documentTypeWithPageNum] ' +
                f'or [classification][documentType] given in event: {event}')
    else:
        raise ValueError(
            f'no [classification] given in event: {event}')

    table = dynamodb.Table(table_name)  #pyright: ignore

    ddb_response = table.get_item(Key={"DOCUMENT_TYPE": document_type_with_page_num})
    logger.debug(f"ddb_response: {ddb_response}")
    input_manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
        event['manifest'])  #type: ignore

    if 'Item' in ddb_response:
        if 'CONFIG' in ddb_response['Item']:
            configuration_manifest: tm.IDPManifest = tm.IDPManifestSchema().loads(
                ddb_response['Item']['CONFIG'])  #type: ignore
            input_manifest.merge(configuration_manifest)
            if configuration_manifest and configuration_manifest.queries_config:
                event['numberOfQueries'] = len(
                    configuration_manifest.queries_config)
            logger.debug(f"merged manifest: {input_manifest}")
            event['manifest'] = tm.IDPManifestSchema().dump(input_manifest)
        else:
            logger.warning("no config found")
    else:
        ddb_response = table.get_item(Key={"DOCUMENT_TYPE": f"{document_type}_default"})
        logger.debug(f"ddb_response: {ddb_response}")
        if 'CONFIG' in ddb_response['Item']:
            configuration_manifest: tm.IDPManifest = tm.IDPManifestSchema().loads(
                ddb_response['Item']['CONFIG'])  #type: ignore
            input_manifest.merge(configuration_manifest)
            if configuration_manifest and configuration_manifest.queries_config:
                event['numberOfQueries'] = len(
                    configuration_manifest.queries_config)
            logger.debug(f"merged manifest: {input_manifest}")
            event['manifest'] = tm.IDPManifestSchema().dump(input_manifest)
        else:
            logger.warning("no config found")

    return event
