# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import time
from random import randint

import boto3
import textractmanifest as tm

from botocore.exceptions import ClientError
from typing import Tuple

logger = logging.getLogger(__name__)
version = "0.0.1"
s3 = boto3.client('s3')
step_functions_client = boto3.client(service_name='stepfunctions')
sqs = boto3.client('sqs')

comprehend = boto3.client("comprehend")


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
    return o.get('Body').read()


def send_failure_to_step_function(error, cause, token, sqs_queue_url, message, receipt_handle):
    try:
        step_functions_client.send_task_failure(taskToken=token,
                                                error=error,
                                                cause=cause)
    except step_functions_client.exceptions.InvalidToken:
        logger.error(f"InvalidToken for message: {message} ")
        sqs.delete_message(QueueUrl=sqs_queue_url,
                           ReceiptHandle=receipt_handle)
    except step_functions_client.exceptions.TaskDoesNotExist:
        logger.error(f"TaskDoesNotExist for message: {message} ")
        sqs.delete_message(QueueUrl=sqs_queue_url,
                           ReceiptHandle=receipt_handle)
    except step_functions_client.exceptions.TaskTimedOut:
        logger.error(f"TaskTimedOut for message: {message} ")
        sqs.delete_message(QueueUrl=sqs_queue_url,
                           ReceiptHandle=receipt_handle)


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'DEBUG')
    logger.setLevel(log_level)
    logger.info(f"version: {version}\n \
                textractmanifest version: {tm.__version__}\n \
                boto3 version: {boto3.__version__}")
    logger.info(json.dumps(event))

    sqs_queue_url = os.environ.get('SQS_QUEUE_URL', None)
    if not sqs_queue_url:
        raise Exception("no SQS_QUEUE_URL set")

    comprehend_classifier_arn = os.environ.get('COMPREHEND_CLASSIFIER_ARN',
                                               None)
    if not comprehend_classifier_arn:
        raise Exception("no COMPREHEND_CLASSIFIER_ARN set")

    sqs_max_retries = int(os.environ.get('SQS_MAX_RETRIES', 3))
    sqs_min_vis_timeout = int(os.environ.get('SQS_MIN_VIS_TIMEOUT', 15))
    sqs_max_vis_timeout = int(os.environ.get('SQS_MAX_VIS_TIMEOUT', 30))

    logger.debug(f"LOG_LEVEL: {log_level} \n \
                COMPREHEND_CLASSIFIER_ARN: {comprehend_classifier_arn} \n \
                SQS_QUEUE_URL: {sqs_queue_url} \n \
                SQS_MAX_RETRIES: {sqs_max_retries} \n \
                SQS_MIN_VIS_TIMEOUT: {sqs_min_vis_timeout} \n \
                SQS_MAX_VIS_TIMEOUT: {sqs_max_vis_timeout}")

    for record in event['Records']:
        if not ("eventSource" in record
                and record["eventSource"]) == "aws:sqs":
            raise ValueError("Unsupported eventSource in record")

        message = json.loads(record["body"])
        token = message['Token']
        execution_id = message['ExecutionId']

        if "Payload" not in message:
            raise ValueError("Need Payload with manifest to process message.")

        receipt_handle = record["receiptHandle"]
        approx_receive_count = int(record["attributes"]["ApproximateReceiveCount"])

        manifest: tm.IDPManifest = tm.IDPManifestSchema().load(
            message["Payload"]['manifest'])  # type: ignore

        s3_path = manifest.s3_path

        logger.info(f"s3_path: {s3_path} \n \
                    token: {token} \n \
                    execution_id: {execution_id}")
        processing_status = True
        try:
            file_bytes = get_file_bytes_from_s3(s3_path=s3_path)

            start_time = round(time.time() * 1000)
            response = comprehend.classify_document(
                Bytes=file_bytes,
                EndpointArn=comprehend_classifier_arn,
                DocumentReaderConfig={
                    'DocumentReadAction': 'TEXTRACT_DETECT_DOCUMENT_TEXT',
                    'DocumentReadMode': 'FORCE_DOCUMENT_READ_ACTION'
                }
            )
            logger.debug(f"comprehend result: {response}")

            classification_result = "NONE"
            for c in response['Classes']:
                if c['Score'] > 0.50:
                    classification_result = c['Name']
                    break

            call_duration = round(time.time() * 1000) - start_time
            logger.info(
                f"comprehend_sync_generic_call_duration_in_ms: {call_duration}"
            )
            try:
                step_functions_client.send_task_success(
                    taskToken=token,
                    output=json.dumps(
                        {"documentType": classification_result}))
            except step_functions_client.exceptions.InvalidToken:
                logger.error(f"InvalidToken for message: {message} ")
                sqs.delete_message(QueueUrl=sqs_queue_url,
                                   ReceiptHandle=receipt_handle)
            except step_functions_client.exceptions.TaskDoesNotExist:
                logger.error(f"TaskDoesNotExist for message: {message} ")
                sqs.delete_message(QueueUrl=sqs_queue_url,
                                   ReceiptHandle=receipt_handle)
            except step_functions_client.exceptions.TaskTimedOut:
                logger.error(f"TaskTimedOut for message: {message} ")
                sqs.delete_message(QueueUrl=sqs_queue_url,
                                   ReceiptHandle=receipt_handle)
            except step_functions_client.exceptions.InvalidOutput:
                # Not sure if to delete here or not, could be a bug in the code that a hot fix could solve,
                # but don't want to retry infinite, which can cause run-away-cost. For now, delete
                logger.error(f"InvalidOutput for message: {message} ")
                sqs.delete_message(QueueUrl=sqs_queue_url,
                                   ReceiptHandle=receipt_handle)

        except comprehend.exceptions.TooManyRequestsException as e:
            # try again, will throw Exception for Lambda and not delete from queue
            logger.error(e, exc_info=True)
            logger.error(f"TooManyRequestsException for: {s3_path} to Comprehend.")
            if approx_receive_count < sqs_max_retries:
                # retry message if less than sqs_max_retries attempts
                processing_status = False
            else:
                send_failure_to_step_function("TooManyRequestsException",
                                              f"{e}\nafter {sqs_max_retries} Lambda retry attempts",
                                              token, sqs_queue_url, message, receipt_handle)
        except comprehend.exceptions.TextSizeLimitExceededException as e:
            logger.error(e, exc_info=True)
            send_failure_to_step_function('TextSizeLimitExceededException', str(e),
                                          token, sqs_queue_url, message, receipt_handle)
        except comprehend.exceptions.InvalidRequestException as e:
            logger.error(e, exc_info=True)
            logger.error(f"InvalidRequestException for: {s3_path} to Comprehend.")
            send_failure_to_step_function("InvalidRequestException", str(e), token,
                                          sqs_queue_url, message, receipt_handle)
        except ClientError as e:
            logger.error(e, exc_info=True)
            if e.response['Error']['Code'] == 'ThrottlingException':
                logger.error(f"ThrottlingException - failed to send: {s3_path} to Comprehend.")
                if approx_receive_count < sqs_max_retries:
                    # retry message if less than sqs_max_retries attempts
                    processing_status = False
                else:
                    send_failure_to_step_function("ThrottlingException",
                                                  f"{e}\nafter {sqs_max_retries} Lambda retry attempts",
                                                  token, sqs_queue_url, message, receipt_handle)
        except Exception as e:
            send_failure_to_step_function('unhandled', str(e), token, sqs_queue_url, message, receipt_handle)

        if not processing_status:
            sqs.change_message_visibility(QueueUrl=sqs_queue_url,
                                          ReceiptHandle=receipt_handle,
                                          VisibilityTimeout=randint(sqs_min_vis_timeout, sqs_max_vis_timeout))
            raise Exception()
