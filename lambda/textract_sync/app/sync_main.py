# Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import time
import boto3
import textractcaller as tc
import textractmanifest as tm

from datetime import datetime
from botocore.config import Config
from typing import List, Dict, Tuple


logger = logging.getLogger(__name__)
__version__ = "0.0.1"
s3 = boto3.client('s3')
step_functions_client = boto3.client(service_name='stepfunctions')

config = Config(retries={'max_attempts': 0, 'mode': 'standard'})
textract = boto3.client("textract", config=config)


def call_textract(manifest: tm.IDPManifest) -> dict:
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(manifest.s3_path)
    params = {
        "Document": {
            'S3Object': {
                'Bucket': s3_bucket,
                'Name': s3_key
            }
        }
    }

    if manifest.textract_features:
        if manifest.queries_config:
            queries: Dict[str, List[Dict[str, str]]] = convert_manifest_queries_config_to_caller(
                manifest.queries_config)
            logger.debug(f"queries: {queries}")
            params["QueriesConfig"] = queries

        params["FeatureTypes"] = manifest.textract_features
        if "QUERIES" in manifest.textract_features and not manifest.queries_config:
            raise ValueError("QUERIES feature requested but queries_config not passed in.")
        logger.debug(f"params: {params}")

        textract_response: dict = textract.analyze_document(**params)

    else:
        logger.debug(f"params: {params}")
        textract_response: dict = textract.detect_document_text(**params)

    return textract_response


def convert_manifest_queries_config_to_caller(queries_config: tm.IDPManifest.queries_config) \
        -> Dict[str, List[Dict[str, str]]]:
    queries: Dict[str, List[Dict[str, str]]] = {'Queries': []}
    for q in queries_config:
        queries['Queries'].append({
            'Text': q.text,
            'Alias': q.alias
        })
    return queries


def split_s3_path_to_bucket_and_key(s3_path: str) -> Tuple[str, str]:
    if len(s3_path) > 7 and s3_path.lower().startswith("s3://"):
        s3_bucket, s3_key = s3_path.replace("s3://", "").split("/", 1)
        return s3_bucket, s3_key
    else:
        raise ValueError(
            f"s3_path: {s3_path} is no s3_path in the form of s3://bucket/key."
        )


class LimitExceededException(Exception):
    pass


class ThrottlingException(Exception):
    pass


class InternalServerError(Exception):
    pass


class ProvisionedThroughputExceededException(Exception):
    pass


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'DEBUG')
    logger.setLevel(log_level)
    logger.info(json.dumps(event))
    logger.info(f"version: {__version__}\n \
        textractmanifest version: {tm.__version__}\n \
        boto3 version: {boto3.__version__}\n \
        textractcaller version: {tc.__version__}.")

    s3_output_bucket = os.environ.get('S3_OUTPUT_BUCKET')
    s3_output_prefix = os.environ.get('S3_OUTPUT_PREFIX')
    textract_api = os.environ.get('TEXTRACT_API', 'GENERIC')

    if not s3_output_bucket or not s3_output_prefix:
        raise ValueError(
            f"no s3_output_bucket: {s3_output_bucket} or s3_output_prefix: {s3_output_prefix} defined."
        )
    logger.debug(f"LOG_LEVEL: {log_level} \n \
                S3_OUTPUT_BUCKET: {s3_output_bucket} \n \
                S3_OUTPUT_PREFIX: {s3_output_prefix} \n \
                TEXTRACT_API: {textract_api} \n  ")

    token = event['Token']
    execution_id = event['ExecutionId']

    if "Payload" not in event:
        raise ValueError("Need Payload with manifest to process message.")

    manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
        event["Payload"]['manifest'])  # type: ignore

    number_of_pages: int = 0
    if 'numberOfPages' in event["Payload"]:
        number_of_pages = int(event['Payload']['numberOfPages'])

    if number_of_pages > 1:
        logger.error("more than 1 page")
        step_functions_client.send_task_failure(
            taskToken=token,
            error='TooManyPagesForSync',
            cause=f'Document with > 1 page was sent to a Sync Textract API endpoint (number pages: {number_of_pages}'
        )

    try:
        start_time = round(time.time() * 1000)
        logger.info(
            f"textract_sync_{textract_api}_number_of_pages_send_to_process: {number_of_pages}"
        )
        
        logger.info(f"s3_path: {manifest.s3_path} \n \
                    token: {token} \n \
                    execution_id: {execution_id}")
        s3_filename, _ = os.path.splitext(os.path.basename(manifest.s3_path))
        logger.debug(f"before call_textract\n \
            input_document: {manifest.s3_path} \n \
            features: {manifest.textract_features}\n \
            queries_config: {manifest.queries_config}")

        textract_response: dict = call_textract(manifest)

        call_duration = round(time.time() * 1000) - start_time
        logger.info(
            f"textract_sync_{textract_api}_call_duration_in_ms: {call_duration}"
        )
        output_bucket_key = s3_output_prefix + "/" + s3_filename + datetime.utcnow(
        ).isoformat() + "/" + s3_filename + ".json"
        s3.put_object(Body=bytes(
            json.dumps(textract_response, indent=4).encode('UTF-8')),
                      Bucket=s3_output_bucket,
                      Key=output_bucket_key)
        logger.info(
            f"textract_sync_{textract_api}_number_of_pages_processed: {number_of_pages}"
        )
        try:
            step_functions_client.send_task_success(
                taskToken=token,
                output=json.dumps({
                    "TextractOutputJsonPath":
                    f"s3://{s3_output_bucket}/{output_bucket_key}"
                }))
        except step_functions_client.exceptions.InvalidToken:
            logger.error(f"InvalidToken for message: {event} ")
        except step_functions_client.exceptions.TaskDoesNotExist:
            logger.error(f"TaskDoesNotExist for message: {event} ")
        except step_functions_client.exceptions.TaskTimedOut:
            logger.error(f"TaskTimedOut for message: {event} ")
        except step_functions_client.exceptions.InvalidOutput:
            # Not sure if to delete here or not, could be a bug in the code that a hot fix could solve,
            # but don't want to retry infinite, which can cause run-away-cost. For now, delete
            logger.error(f"InvalidOutput for message: {event} ")

    except textract.exceptions.InvalidS3ObjectException:
        cause = f"InvalidS3ObjectException for object: {manifest}"
        error = "InvalidS3ObjectException"
        logger.error(cause)
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause[:250])
    except textract.exceptions.InvalidParameterException:
        error = f"InvalidParameterException"
        cause = f"textract.exceptions.InvalidParameterException: for manifest: {manifest}"
        logger.error(cause)
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause[:250])
    except textract.exceptions.InvalidKMSKeyException:
        cause = f"textract.exceptions.InvalidKMSKeyException: for manifest: {manifest}"
        logger.error(cause)
        error = f"UnsupportedDocumentException",
        cause = f"textract.exceptions.UnsupportedDocumentException: for manifest: {manifest}"
        logger.error(cause)
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause[:250])
    except textract.exceptions.DocumentTooLargeException:
        error = "DocumentTooLargeException"
        cause = f"textract.exceptions.DocumentTooLargeException: for manifest: {manifest}"
        logger.error(cause)
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause[:250])
    except textract.exceptions.BadDocumentException:
        error = "BadDocumentException"
        cause = f"textract.exceptions.BadDocumentException: for manifest: {manifest}"
        logger.error(cause)
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause[:250])
    except textract.exceptions.AccessDeniedException:
        error = "AccessDeniedException"
        cause = f"textract.exceptions.AccessDeniedException: for manifest: {manifest}"
        logger.error(cause)
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause[:250])
    except textract.exceptions.IdempotentParameterMismatchException:
        error = "IdempotentParameterMismatchException"
        cause = f"textract.exceptions.IdempotentParameterMismatchException: for manifest: {manifest}"
        logger.error(cause)
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause[:250])
    # these Exceptions we can retry, so we put them back on the queue
    except textract.exceptions.ProvisionedThroughputExceededException:
        logger.warning(
            f"textract.exceptions.ProvisionedThroughputExceededException")
        raise ProvisionedThroughputExceededException(
            'ProvisionedThroughputExceededException')
    except textract.exceptions.InternalServerError:
        logger.warning(f"textract.exceptions.InternalServerError")
        raise InternalServerError('InternalServerError')
    except textract.exceptions.ThrottlingException:
        logger.warning(f"textract.exceptions.ThrottlingException")
        raise ThrottlingException('ThrottlingException')
    except textract.exceptions.LimitExceededException:
        logger.warning(f"textract.exceptions.LimitExceededException")
        raise LimitExceededException('LimitExceededException')
    except Exception as e:
        error = "not_handled_exception"
        cause = str(e)
        try:
            step_functions_client.send_task_failure(taskToken=token,
                                                    error=error,
                                                    cause=cause[:250])
        except step_functions_client.exceptions.InvalidToken:
            logger.error(f"InvalidToken for message: {event} ")
        except step_functions_client.exceptions.TaskDoesNotExist:
            logger.error(f"TaskDoesNotExist for message: {event} ")
        except step_functions_client.exceptions.TaskTimedOut:
            logger.error(f"TaskTimedOut for message: {event} ")
            logger.error(cause)
            logger.error(e)
