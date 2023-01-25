# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import io
import csv
import boto3
from typing import Tuple, List, Dict
import json
from textractprettyprinter.t_pretty_print import convert_queries_to_list_trp2, \
    convert_form_to_list_trp2, \
    convert_table_to_list
import trp.trp2 as t2
import trp
import datetime

logger = logging.getLogger(__name__)
version = "0.0.3"
s3_client = boto3.client('s3')
step_functions_client = boto3.client(service_name='stepfunctions')


def get_signature_table_info(file_json: dict)\
        -> Tuple[bool, Dict[str, dict], List[dict]]:
    has_signature: bool = False
    signature_block_ids: List[str] = list()
    signature_blocks: List[dict] = list()
    table_blocks: List[dict] = list()
    block_map: Dict[str, dict] = dict()
    page_block: dict = dict()

    for block in file_json['Blocks']:
        if block['BlockType'] == "SIGNATURE":
            has_signature = True
            signature_block_ids.append(block['Id'])
            signature_blocks.append(block)
        elif block['BlockType'] == "TABLE":
            table_blocks.append(block)
        elif block['BlockType'] == "PAGE":
            page_block = block
        block_map[block['Id']] = block

    for block in signature_blocks:
        file_json['Blocks'].remove(block)

    if page_block:
        for _id in signature_block_ids:
            page_block['Relationships'][0]['Ids'].remove(_id)
    else:
        raise ValueError(f"PAGE block not found in {file_json['Blocks']}")

    return has_signature, block_map, table_blocks


def get_table_list(block_map: Dict[str, dict], table_blocks: List[dict]) -> List[List]:
    result_list: List[List] = list()
    for table_block in table_blocks:
        table: trp.Table = trp.Table(table_block, block_map)
        table_list: List = convert_table_to_list(table)
        result_list.append(table_list)
    return result_list


def split_s3_path_to_bucket_and_key(s3_path: str) -> Tuple[str, str]:
    if len(s3_path) > 7 and s3_path.lower().startswith("s3://"):
        s3_bucket, s3_key = s3_path.replace("s3://", "").split("/", 1)
        return s3_bucket, s3_key
    else:
        raise ValueError(
            f"s3_path: {s3_path} is no s3_path in the form of s3://bucket/key."
        )


def get_file_from_s3(s3_path: str) -> bytes:
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(s3_path)
    o = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    return o.get('Body').read()


def lambda_handler(event, _):
    # takes and even which includes a location to a Textract JSON schema file
    # and generates CSV based on Query results + FORMS + TABLES results
    # in the form of
    # filename, page, datetime, key, value

    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.debug(f"version: {version}")
    logger.debug(json.dumps(event))
    csv_s3_output_prefix = os.environ.get('CSV_S3_OUTPUT_PREFIX')
    output_type = os.environ.get('OUTPUT_TYPE', 'CSV')
    csv_s3_output_bucket = os.environ.get('CSV_S3_OUTPUT_BUCKET')

    logger.info(f"CSV_S3_OUTPUT_PREFIX: {csv_s3_output_prefix} \n\
        CSV_S3_OUTPUT_BUCKET: {csv_s3_output_bucket} \n\
            OUTPUT_TYPE: {output_type}")
    task_token = event['Token']
    try:
        if not csv_s3_output_prefix or not csv_s3_output_bucket:
            raise ValueError(
                f"require CSV_S3_OUTPUT_PREFIX and CSV_S3_OUTPUT_BUCKET")
        if 'Payload' not in event and 'textract_result' in event[
                'Payload'] and 'TextractOutputJsonPath' not in event[
                    'Payload']['textract_result']:
            raise ValueError(
                f"no 'TextractOutputJsonPath' in event['textract_result]")
        # FIXME: hard coded result location
        s3_path = event['Payload']['textract_result']['TextractOutputJsonPath']
        classification = ""
        if 'classification' in event['Payload'] and event['Payload'][
                'classification'] and 'documentType' in event['Payload'][
                    'classification']:
            classification = event['Payload']['classification']['documentType']

        base_filename = os.path.basename(s3_path)
        base_filename_no_suffix, _ = os.path.splitext(base_filename)
        file_json = json.loads(get_file_from_s3(s3_path=s3_path).decode('utf-8'))

        timestamp = datetime.datetime.now().astimezone().replace(
            microsecond=0).isoformat()
        result_value = ""
        if output_type == 'CSV':
            has_signature, block_map, table_blocks = get_signature_table_info(file_json)
            trp2_doc: t2.TDocument = t2.TDocumentSchema().load(
                file_json)  # type: ignore

            key_value_list = convert_form_to_list_trp2(trp2_doc=trp2_doc)  # type: ignore
            table_value_list = get_table_list(block_map, table_blocks)  # type: ignore
            queries_value_list = convert_queries_to_list_trp2(trp2_doc=trp2_doc)  # type: ignore
            
            csv_output = io.StringIO()
            csv_writer = csv.writer(csv_output,
                                    delimiter=",",
                                    quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            for page in key_value_list:
                csv_writer.writerows(
                    [[timestamp, classification, base_filename, "FORMS"] +
                     [x[1], x[3]] for x in page]) # only include key name and key value
            for page in queries_value_list:
                csv_writer.writerows(
                    [[timestamp, classification, base_filename, "QUERIES"] +
                     [x[1], x[3]] for x in page]) # only include alias and query result
            for page in table_value_list:
                csv_writer.writerows(
                    [[timestamp, classification, base_filename, "TABLES"] + x
                     for x in page])
            if has_signature:
                signature_value = "Contains Signature"
            else:
                signature_value = "Does NOT Contain Signature"
            csv_writer.writerow(
                [timestamp, classification, base_filename,
                 "SIGNATURES", "HAS_SIGNATURE", signature_value])
            csv_s3_output_key = f"{csv_s3_output_prefix}/{timestamp}/{base_filename_no_suffix}.csv"
            result_value = csv_output.getvalue()
        elif output_type == 'LINES':
            csv_s3_output_key = f"{csv_s3_output_prefix}/{timestamp}/{base_filename_no_suffix}.txt"
            trp2_doc: t2.TDocument = t2.TDocumentSchema().load(
                file_json)  # type: ignore
            for page in trp2_doc.pages:
                result_value += t2.TDocument.get_text_for_tblocks(
                    trp2_doc.lines(page=page))
            logger.debug(f"got {len(result_value)}")
        else:
            raise ValueError(f"output_type '${output_type}' not supported: ")

        s3_client.put_object(Body=bytes(result_value.encode('UTF-8')),
                             Bucket=csv_s3_output_bucket,
                             Key=csv_s3_output_key)
        logger.debug(
            f"TextractOutputCSVPath: s3://{csv_s3_output_bucket}/{csv_s3_output_key}"
        )
        step_functions_client.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                "TextractOutputCSVPath":
                f"s3://{csv_s3_output_bucket}/{csv_s3_output_key}"
            }))
    except Exception as e:
        logger.error(e, exc_info=True)
        step_functions_client.send_task_failure(taskToken=task_token,
                                                error=str(type(e)),
                                                cause=str(e))
