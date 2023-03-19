from constructs import Construct
import os
import json
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3n
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_iam as iam
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.custom_resources as custom_resources
from aws_cdk import (CfnOutput, RemovalPolicy, Stack, Duration, Aws, CustomResource)
import amazon_textract_idp_cdk_constructs as tcdk


class DocumentSplitterWorkflow(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        script_location = os.path.dirname(__file__)
        s3_upload_prefix = "uploads"
        s3_output_prefix = "textract-output"
        s3_csv_output_prefix = "textract-csv-output"
        s3_joined_output_prefix = "textract-joined-output"
        comprehend_classifier_endpoint = \
            "arn:aws:comprehend:<REGION>:<ACCOUNT_ID>:document-classifier-endpoint/<CLASSIFIER_NAME>"

        # BEWARE! This is a demo/POC setup, remove the auto_delete_objects=True
        # to make sure the data is not lost
        # S3 bucket
        document_bucket = s3.Bucket(
            self,
            "TextractSimpleSyncWorkflow",
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY)
        s3_output_bucket = document_bucket.bucket_name
        workflow_name = "DocumentSplitterWorkflow"

        # DEFINE Lambda functions, policies, DynamoDB table, Provider, CustomResource

        lambda_comprehend_sync = lambda_.DockerImageFunction(
            self,
            'ComprehendSyncCall',
            code=lambda_.DockerImageCode.from_image_asset(os.path.join(script_location,
                                                                       '../lambda/comprehend_sync/')),
            memory_size=256,
            timeout=Duration.seconds(60),
            environment={
                "LOG_LEVEL": "DEBUG",
                "COMPREHEND_CLASSIFIER_ARN": comprehend_classifier_endpoint,
                "TEXT_OR_BYTES": "BYTES",
                "DOCUMENT_READER_CONFIG": json.dumps({
                    "DocumentReadAction": "TEXTRACT_DETECT_DOCUMENT_TEXT",
                    "DocumentReadMode": "FORCE_DOCUMENT_READ_ACTION"
                })
            }
        )
        lambda_comprehend_sync.add_to_role_policy(iam.PolicyStatement(
            actions=['comprehend:ClassifyDocument'], resources=['*']
        ))
        lambda_comprehend_sync.add_to_role_policy(iam.PolicyStatement(
            actions=["textract:Analyze*", "textract:Detect*"],
            resources=["*"]
        ))
        lambda_comprehend_sync.add_to_role_policy(iam.PolicyStatement(
            actions=['s3:GetObject', 's3:ListBucket', 's3:PutObject'],
            resources=[f"arn:aws:s3:::{s3_output_bucket}", f"arn:aws:s3:::{s3_output_bucket}/*"]
        ))

        lambda_comprehend_sync.add_to_role_policy(iam.PolicyStatement(
            actions=['states:SendTaskFailure', 'states:SendTaskSuccess'], resources=['*']
        ))

        lambda_textract_sync: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaTextractSync",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/textract_sync/')),
            memory_size=300,
            timeout=Duration.seconds(300),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "LOG_LEVEL": "DEBUG",
                "S3_OUTPUT_BUCKET": s3_output_bucket,
                "S3_OUTPUT_PREFIX": s3_output_prefix,
                "TEXTRACT_API": "GENERIC"})
        lambda_textract_sync.add_to_role_policy(
            iam.PolicyStatement(
                actions=["textract:Analyze*", "textract:Detect*"],
                resources=["*"]))
        lambda_textract_sync.add_to_role_policy(
            iam.PolicyStatement(
                actions=['s3:GetObject', 's3:ListBucket', 's3:PutObject'],
                resources=[f"arn:aws:s3:::{s3_output_bucket}", f"arn:aws:s3:::{s3_output_bucket}/*"]))
        lambda_textract_sync.add_to_role_policy(
            iam.PolicyStatement(
                actions=['states:SendTaskFailure', 'states:SendTaskSuccess'],
                resources=["*"]))

        lambda_generate_csv: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaGenerateCSV",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/generatecsv/')),
            memory_size=1048,
            timeout=Duration.minutes(15),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "LOG_LEVEL": "DEBUG",
                "CSV_S3_OUTPUT_BUCKET": s3_output_bucket,
                "CSV_S3_OUTPUT_PREFIX": s3_csv_output_prefix,
                "OUTPUT_TYPE": "CSV"})
        lambda_generate_csv.add_to_role_policy(
            iam.PolicyStatement(
                actions=['s3:Get*', 's3:List*', 's3:PutObject'],
                resources=[f"arn:aws:s3:::{s3_output_bucket}", f"arn:aws:s3:::{s3_output_bucket}/*"]))
        lambda_generate_csv.add_to_role_policy(
            iam.PolicyStatement(
                actions=['states:SendTaskFailure', 'states:SendTaskSuccess'],
                resources=["*"]))

        lambda_enumerate_pages: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaEnumeratePages",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/enumerate_pages/')),
            memory_size=1024,
            timeout=Duration.seconds(180),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "LOG_LEVEL": "DEBUG"})

        configuration_table = dynamodb.Table(
            self,
            "TextractConfigurationTable",
            partition_key=dynamodb.Attribute(
                name="DOCUMENT_TYPE", type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST)

        lambda_config_prefill: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaConfigurationPrefill",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/config_prefill/')),
            memory_size=300,
            timeout=Duration.seconds(300),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "LOG_LEVEL": "DEBUG",
                "CONFIGURATION_TABLE": configuration_table.table_name})
        lambda_config_prefill.add_to_role_policy(
            iam.PolicyStatement(
                actions=['dynamodb:PutItem', 'dynamodb:GetItem'],
                resources=[configuration_table.table_arn]))
        lambda_config_prefill.node.add_dependency(configuration_table)

        provider = custom_resources.Provider(
            self,
            "Provider",
            on_event_handler=lambda_config_prefill)
        CustomResource(self, "Resource", service_token=provider.service_token)

        lambda_configurator: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaClassificationConfigurator",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/configurator/')),
            memory_size=1024,
            timeout=Duration.seconds(900),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "LOG_LEVEL": "DEBUG",
                "CONFIGURATION_TABLE": configuration_table.table_name})
        lambda_configurator.add_to_role_policy(
            iam.PolicyStatement(
                actions=['dynamodb:PutItem', 'dynamodb:GetItem'],
                resources=[configuration_table.table_arn]))

        lambda_generate_classification_mapping: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaGenerateClassificationMapping",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/map_classifications_lambda/')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64)

        lambda_compile_paths: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaCompilePaths",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/compile_paths/')),
            memory_size=1024,
            timeout=Duration.seconds(180),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "LOG_LEVEL": "DEBUG"})

        lambda_join_csv: lambda_.IFunction = lambda_.DockerImageFunction(
            self,
            "LambdaJoinCSV",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location,
                             '../lambda/join_csv/')),
            memory_size=1024,
            architecture=lambda_.Architecture.X86_64,
            timeout=Duration.seconds(180),
            environment={
                "LOG_LEVEL": "DEBUG",
                "JOINED_S3_OUTPUT_BUCKET": s3_output_bucket,
                "JOINED_S3_OUTPUT_PREFIX": s3_joined_output_prefix
            })
        lambda_join_csv.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:Get*", "s3:List*", "s3:PutObject"],
                resources=[f"arn:aws:s3:::{s3_output_bucket}", f"arn:aws:s3:::{s3_output_bucket}/*"]))

        # DEFINE Step Functions tasks ###############
        # Step Functions task to set document mime type and number of pages
        decider_task = tcdk.TextractPOCDecider(
            self,
            f"{workflow_name}-Decider",
        )

        # Step Functions task to split documents into single pages
        document_splitter_task = tcdk.DocumentSplitter(
            self,
            "TaskDocumentSplitter",
            s3_output_bucket=s3_output_bucket,
            s3_output_prefix=s3_output_prefix)

        # Step Functions task to call Textract
        comprehend_sync_task = tasks.LambdaInvoke(
            self,
            'TaskClassification',
            lambda_function=lambda_comprehend_sync,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            timeout=Duration.hours(24),
            payload=sfn.TaskInput.from_object({
                "Token":
                sfn.JsonPath.task_token,
                "ExecutionId":
                sfn.JsonPath.string_at('$$.Execution.Id'),
                "Payload":
                sfn.JsonPath.entire_payload,
            }),
            result_path="$.classification")
        comprehend_sync_task.add_retry(
            max_attempts=100,
            backoff_rate=1.1,
            interval=Duration.seconds(1),
            errors=['ThrottlingException', 'LimitExceededException',
                    'InternalServerError', 'ProvisionedThroughputExceededException']
        )

        enumerate_pages_task = tasks.LambdaInvoke(
            self,
            "TaskEnumeratePages",
            lambda_function=lambda_enumerate_pages)

        # Step Functions task to configure Textract call based on classification result
        configurator_task = tasks.LambdaInvoke(
            self,
            f"{workflow_name}-Configurator",
            lambda_function=lambda_configurator,
            timeout=Duration.seconds(100),
            output_path='$.Payload')

        # Step Functions task to call Textract
        textract_sync_queries_task = tasks.LambdaInvoke(
            self,
            "TaskTextractSyncQueries",
            lambda_function=lambda_textract_sync,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            timeout=Duration.hours(24),
            payload=sfn.TaskInput.from_object({
                "Token":
                sfn.JsonPath.task_token,
                "ExecutionId":
                sfn.JsonPath.string_at('$$.Execution.Id'),
                "Payload":
                sfn.JsonPath.entire_payload,
            }),
            result_path="$.textract_result")
        textract_sync_queries_task.add_retry(
            max_attempts=1,
            backoff_rate=1,
            interval=Duration.seconds(1),
            errors=['ThrottlingException', 'LimitExceededException',
                    'InternalServerError', 'ProvisionedThroughputExceededException'])

        # Generate CSV from Textract JSON
        generate_csv_task = tasks.LambdaInvoke(
            self,
            "TaskGenerateCSV",
            lambda_function=lambda_generate_csv,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            timeout=Duration.hours(24),
            payload=sfn.TaskInput.from_object({
                "Token":
                sfn.JsonPath.task_token,
                "ExecutionId":
                sfn.JsonPath.string_at('$$.Execution.Id'),
                "Payload":
                sfn.JsonPath.entire_payload
            }),
            result_path="$.csv_output_location")

        generate_classification_mapping_task = tasks.LambdaInvoke(
            self,
            "TaskGenerateClassificationMapping",
            lambda_function=lambda_generate_classification_mapping,
            output_path='$.Payload')

        compile_paths_task = tasks.LambdaInvoke(
            self,
            "TaskCompilePaths",
            lambda_function=lambda_compile_paths)

        join_csv_task = tasks.LambdaInvoke(
            self,
            "TaskJoinCSV",
            lambda_function=lambda_join_csv,
            output_path='$.Payload')

        # Step Functions Flow Definition #########

        # Routing based on document type
        doc_type_choice = sfn.Choice(self, 'RouteDocType') \
                       .when(sfn.Condition.string_equals('$.classification.documentType', 'NONE'),
                             sfn.Fail(self, "DocumentTypeNotImplemented")) \
                       .otherwise(sfn.Pass(self, "PassState"))

        # Map state to classify pages in parallel
        # Creates manifest
        # Generates S3 path from S3 Document Splitter Output Bucket and Output Path
        classify_pages_map = sfn.Map(
            self,
            "ClassifyPagesMapState",
            items_path=sfn.JsonPath.string_at('$.pages'),
            parameters={
                "manifest": {
                    "s3Path":
                    sfn.JsonPath.string_at("States.Format('s3://{}/{}/{}', \
                  $.documentSplitterS3OutputBucket, \
                  $.documentSplitterS3OutputPath, \
                  $$.Map.Item.Value)")
                },
                "mime": sfn.JsonPath.string_at('$.mime'),
                "numberOfPages": 1})

        # Map state to compile each page's CSV into one CSV document
        process_pages_map = sfn.Map(
            self,
            "ProcessPagesMapState",
            items_path=sfn.JsonPath.string_at('$.Payload'))

        # Map state to compile each page's CSV into one CSV document
        compile_pages_map = sfn.Map(
            self,
            "CompilePagesMapState",
            items_path=sfn.JsonPath.string_at('$.Payload'))

        # Classify and route
        comprehend_sync_task.next(doc_type_choice)
        classify_pages_map.iterator(comprehend_sync_task)

        configurator_task.next(textract_sync_queries_task) \
            .next(generate_csv_task) \
            .next(generate_classification_mapping_task)
        process_pages_map.iterator(configurator_task)

        workflow_chain = sfn.Chain \
            .start(decider_task) \
            .next(document_splitter_task) \
            .next(classify_pages_map) \
            .next(enumerate_pages_task) \
            .next(process_pages_map) \
            .next(compile_paths_task) \
            .next(compile_pages_map)

        compile_pages_map.iterator(join_csv_task)

        state_machine = sfn.StateMachine(self,
                                         workflow_name,
                                         definition=workflow_chain)

        # Step Functions definition end ###############

        # Lambda function to start workflow on new object at S3 bucket/prefix location
        lambda_step_start_step_function = lambda_.DockerImageFunction(
            self,
            "LambdaStartStepFunctionGeneric",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(script_location, '../lambda/startstepfunction')),
            memory_size=128,
            architecture=lambda_.Architecture.X86_64,
            environment={"STATE_MACHINE_ARN": state_machine.state_machine_arn})

        lambda_step_start_step_function.add_to_role_policy(
            iam.PolicyStatement(actions=['states:StartExecution'],
                                resources=[state_machine.state_machine_arn]))

        document_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(
                lambda_step_start_step_function),  # type: ignore
            s3.NotificationKeyFilter(prefix=s3_upload_prefix))

        # CloudFormation OUTPUT
        CfnOutput(
            self,
            "DocumentUploadLocation",
            value=f"s3://{document_bucket.bucket_name}/{s3_upload_prefix}/",
            export_name=f"{Aws.STACK_NAME}-DocumentUploadLocation")
        CfnOutput(
            self,
            "StartStepFunctionLambdaLogGroup",
            value=lambda_step_start_step_function.log_group.log_group_name)
        current_region = Stack.of(self).region
        CfnOutput(
            self,
            'StepFunctionFlowLink',
            value=
            f"https://{current_region}.console.aws.amazon.com/states/home?region={current_region}#/statemachines/view/{state_machine.state_machine_arn}",
            export_name=f"{Aws.STACK_NAME}-StepFunctionFlowLink")
