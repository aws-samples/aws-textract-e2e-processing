# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import time

import boto3
import textractmanifest as tm

from botocore.exceptions import ClientError
from typing import Tuple, List

logger = logging.getLogger(__name__)
version = "0.0.1"
s3 = boto3.client("s3")
step_functions_client = boto3.client(service_name="stepfunctions")
sqs = boto3.client("sqs")

comprehend = boto3.client("comprehend")


def validate_document_reader_config(document_reader_config: dict):
    document_read_action = document_reader_config.get("DocumentReadAction", None)
    if document_read_action not in {"TEXTRACT_DETECT_DOCUMENT_TEXT", "TEXTRACT_ANALYZE_DOCUMENT"}:
        raise Exception("""DocumentReadAction must be 
                                       TEXTRACT_DETECT_DOCUMENT_TEXT or TEXTRACT_ANALYZE_DOCUMENT""")

    document_read_mode = document_reader_config.get("DocumentReadMode", "FORCE_DOCUMENT_READ_ACTION")
    if document_read_mode not in {"FORCE_DOCUMENT_READ_ACTION", "SERVICE_DEFAULT"}:
        raise Exception("DocumentReadMode must be one of FORCE_DOCUMENT_READ_ACTION or SERVICE_DEFAULT")

    feature_types: List[str] = document_reader_config.get("FeatureTypes", None)
    if document_read_action == "TEXTRACT_ANALYZE_DOCUMENT":
        if not feature_types:
            raise Exception("FeatureTypes must be set when DocumentReadAction is TEXTRACT_ANALYZE_DOCUMENT")
        elif not isinstance(feature_types, list) \
                or not (1 <= len(feature_types) <= 2) \
                or not all([feature_type in {"TABLES", "FORMS"} for feature_type in feature_types]):
            raise Exception("FeatureTypes must be a list of strings with values TABLES and/or FORMS")


def split_s3_path_to_bucket_and_key(s3_path: str) -> Tuple[str, str]:
    if len(s3_path) > 7 and s3_path.lower().startswith("s3://"):
        s3_bucket, s3_key = s3_path.replace("s3://", "").split("/", 1)
        return s3_bucket, s3_key
    else:
        raise ValueError(
            f"s3_path: {s3_path} is no s3_path in the form of s3://bucket/key."
        )


def get_file_bytes_from_s3(s3_path: str) -> bytes:
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(s3_path)
    o = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    return o.get("Body").read()


def send_failure_to_step_function(error, cause, token, event):
    try:
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause)
    except step_functions_client.exceptions.InvalidToken:
        logger.error(f"InvalidToken for event: {event} ")
    except step_functions_client.exceptions.TaskDoesNotExist:
        logger.error(f"TaskDoesNotExist for event: {event} ")
    except step_functions_client.exceptions.TaskTimedOut:
        logger.error(f"TaskTimedOut for event: {event} ")


class TooManyRequestsException(Exception):
    pass


class ThrottlingException(Exception):
    pass


def lambda_handler(event, _):
    log_level = os.environ.get("LOG_LEVEL", "DEBUG")
    logger.setLevel(log_level)
    logger.info(f"version: {version}\n \
                textractmanifest version: {tm.__version__}\n \
                boto3 version: {boto3.__version__}")
    logger.info(json.dumps(event))

    comprehend_classifier_arn = os.environ.get("COMPREHEND_CLASSIFIER_ARN", None)
    if not comprehend_classifier_arn:
        raise Exception("no COMPREHEND_CLASSIFIER_ARN set")

    text_or_bytes = os.environ.get("TEXT_OR_BYTES", "TEXT")
    document_reader_config = os.environ.get("DOCUMENT_READER_CONFIG", None)
    if text_or_bytes in {"TEXT", "BYTES"}:
        if text_or_bytes == "BYTES":
            if not document_reader_config:
                raise Exception("no DOCUMENT_READER_CONFIG set")
            document_reader_config = json.loads(document_reader_config)
            validate_document_reader_config(document_reader_config)
    else:
        raise Exception("TEXT_OR_BYTES must be either TEXT or BYTES")

    logger.debug(f"LOG_LEVEL: {log_level} \n \
                COMPREHEND_CLASSIFIER_ARN: {comprehend_classifier_arn} \n \
                TEXT_OR_BYTES: {text_or_bytes} \n \
                DOCUMENT_READER_CONFIG: {document_reader_config}")

    token = event["Token"]
    execution_id = event["ExecutionId"]

    if "Payload" not in event:
        raise ValueError("Need Payload with manifest to process event.")

    manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
        event["Payload"]["manifest"])  # type: ignore

    s3_path = manifest.s3_path
    payload = event["Payload"]

    if text_or_bytes == "TEXT":
        if 'txt_output_location' in payload and "TextractOutputCSVPath" in payload['txt_output_location']:
            s3_input_text = payload['txt_output_location']['TextractOutputCSVPath']
        else:
            raise ValueError(
                f"no ['txt_output_location']['TextractOutputCSVPath'] to get the text file from "
            )

    logger.info(f"s3_path: {s3_path} \n \
                    s3_input_text: {s3_input_text if text_or_bytes == 'TEXT' else None} \n\
                    token: {token} \n \
                    execution_id: {execution_id}")

    try:
        params = {"EndpointArn": comprehend_classifier_arn}
        if text_or_bytes == "TEXT":
            text = get_file_bytes_from_s3(s3_path=s3_input_text).decode('utf-8')[0:4900]
            params["Text"] = text
        elif text_or_bytes == "BYTES":
            file_bytes = get_file_bytes_from_s3(s3_path=s3_path)
            params["Bytes"] = file_bytes
            params["DocumentReaderConfig"] = document_reader_config

        start_time = round(time.time() * 1000)
        response = comprehend.classify_document(**params)
        logger.debug(f"comprehend result: {response}")

        classification_result = "NONE"
        for c in response["Classes"]:
            if c["Score"] > 0.50:
                classification_result = c["Name"]
                break

        call_duration = round(time.time() * 1000) - start_time
        logger.info(
            f"comprehend_sync_generic_call_duration_in_ms: {call_duration}"
        )
        try:
            step_functions_client.send_task_success(
                taskToken=token,
                output=json.dumps({"documentType": classification_result}))
        except step_functions_client.exceptions.InvalidToken:
            logger.error(f"InvalidToken for event: {event} ")
        except step_functions_client.exceptions.TaskDoesNotExist:
            logger.error(f"TaskDoesNotExist for event: {event} ")
        except step_functions_client.exceptions.TaskTimedOut:
            logger.error(f"TaskTimedOut for event: {event} ")
        except step_functions_client.exceptions.InvalidOutput:
            logger.error(f"InvalidOutput for event: {event} ")

    except comprehend.exceptions.TextSizeLimitExceededException as e:
        logger.error(e, exc_info=True)
        send_failure_to_step_function('TextSizeLimitExceededException', str(e), token, event)
    except comprehend.exceptions.InvalidRequestException as e:
        logger.error(e, exc_info=True)
        send_failure_to_step_function('InvalidRequestException', str(e), token, event)
    except comprehend.exceptions.TooManyRequestsException:
        # try again, will throw Exception for Lambda and retry
        logger.warning(f"TooManyRequestsException for: {s3_path} to Comprehend.")
        raise TooManyRequestsException('TooManyRequestsException')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ThrottlingException':
            logger.warning(f"ThrottlingException - failed to send: {s3_path} to Comprehend.")
            raise ThrottlingException('ThrottlingException')
        else:
            logger.error(e, exc_info=True)
            send_failure_to_step_function('ClientError', str(e), token, event)
    except Exception as e:
        send_failure_to_step_function('unhandled', str(e), token, event)
