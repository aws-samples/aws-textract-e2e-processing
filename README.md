# Amazon Textract IDP - End to End Document Processing 

# Deployment

This workflow is heavily based on the [Amazon Textract IDP CDK Constructs](https://github.com/aws-samples/amazon-textract-idp-cdk-constructs/) as well as the [Amazon Textract IDP CDK Stack Samples.](https://github.com/aws-samples/amazon-textract-idp-cdk-stack-samples)

The samples use the [AWS Cloud Development Kit (AWS CDK)](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html).
Also it requires Docker.

To set up your environment:

## Download the AWS CLI

Download the latest AWS CLI version
```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
```

Unzip the file.
```
unzip awscliv2.zip
```

Then install it.
```
sudo ./aws/install
```

And as a last step, check the version.
```
aws --version
```



## Clone the Repository

```
git clone https://github.com/aws-samples/aws-textract-e2e-processing.git
cd aws-textract-e2e-processing/
```

## Document Splitter Workflow Setup

There will be two areas to modify. In our demo today will be using Health Insurance Forms.

### 1. Comprehend Classifier
If you want to train a classifier with the provided medical documents used in the demo, you can use the [comprehend_claims_dataset.csv](https://github.com/aws-samples/aws-textract-e2e-processing/blob/main/comprehend_claims_dataset.csv) file in this repo. Refer to the [Amazon Comprehend documentation](https://docs.aws.amazon.com/comprehend/latest/dg/training-classifier-model.html) for more information on how to train a classifier.

Alternatively, you can train your own classifier with a set of documents using workflow1 of this project, you can train your own Comprehend Classifier: 
https://github.com/aws-samples/aws-document-classifier-and-splitter.

After training your classifier and creating an endpoint, you should have a Comprehend Custom Classification Endpoint ARN. 

Navigate to ```docsplitter/document_split_workflow.py``` and modify lines 26-27 which contain ```comprehend_classifier_endpoint```. Paste in your endpoint ARN [in this line](https://github.com/aws-samples/aws-textract-e2e-processing/blob/main/docsplitter/document_split_workflow.py#L27). It should be in the form: 

```arn:aws:comprehend:<REGION>:<ACCOUNT_ID>:document-classifier-endpoint/<CLASSIFIER_NAME>```.


### 2. Document Configuration Table
For each of the document types you trained, you must specify its ```queries``` and ```textract_features```.

```queries```: a list of Queries. For more information, see the [QueriesConfig Documentation](https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html#Textract-AnalyzeDocument-request-QueriesConfig).

```textract_features```: a list of the Textract features you want to extract from the document. Can one or more of 'TABLES' | 'FORMS' | 'QUERIES' | 'SIGNATURES'.
For more information, see the [FeatureTypes Documentation](https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html#Textract-AnalyzeDocument-request-FeatureTypes).

Navigate to [```lambda/config_prefill/app/generate_csv.py```](https://github.com/aws-samples/aws-textract-e2e-processing/blob/main/lambda/config_prefill/app/generate_csv.py). Each document type needs its ```classification```, ```queries```, and ```textract_features``` configured, as shown in the examples there by creating ```CSVRow``` instances.

## Install dependencies

Now you install the project dependencies:
```
python -m pip install -r requirements.txt
```

And initialize the account and region for the CDK. This will create the S3 buckets and roles for the CDK tool to store artifacts and to be able to deploy infrastructure.
```
cdk bootstrap
```

## Deploy Stack

Once the Comprehend Classifier and Document Configuration Table are set, deploy using
```
cdk deploy DocumentSplitterWorkflow --outputs-file document_splitter_outputs.json --require-approval never 
```
## Upload the Document
Verify that the stack is fully deployed.


Then in the terminal window, execute the aws s3 cp command to upload the document to the DocumentUploadLocation for the DocumentSplitterWorkflow.

```
aws s3 cp sample-doc.pdf $(aws cloudformation list-exports --query 'Exports[?Name==`DocumentSplitterWorkflow-DocumentUploadLocation`].Value' --output text)
```
 
## Open the AWS Step Functions Execution Page
Now open the Step Function workflow. You can get the Step Function flow link from the document_splitter_outputs.json file or browse to the AWS Console and select Step Functions or use the following command to get the link.

```
aws cloudformation list-exports --query 'Exports[?Name==`DocumentSplitterWorkflow-StepFunctionFlowLink`].Value' --output text
```

then click on it and open

Clicking on the "Execution input & output tab" at the top show the overall input and overall output from the entire flow. The Map state output combines all individual results into an array:

```
[
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/claimform_pages_1-1_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "claimform_page1": [
        "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/tables/claimform_page1/table_1.csv"
      ],
    }
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/dischargesummary_pages_2-2_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "dischargesummary_page1": []
    }
  },
  {
    "JoinedCSVOutputPath": "s3://<S3_OUTPUT_BUCKET>/<S3_JOINED_OUTPUT_PREFIX>/csvfiles_<EXECUTION_NAME>/doctorsnote_pages_3-3_<TIMESTAMP>.csv",
    "TextractOutputTablesPaths": {
      "doctorsnote_page1": []
    }
  }
]
```

Each of the outputted CSV files is the result of parsing its original pages' Textract AnalyzeDocument output. Each page also includes whether or not it contains a signature. 

FORMS, QUERIES, and SIGNATURES are in the joined CSV files. 

TABLES are split into multiple CSV files, one for each table. These files are in a subfolder within the ```tables``` folder, where the subfolder is named after the document type and page number that the table is extracted from. For example, all tables extracted from the HICFA claim form are in the ```claimform_page1``` subfolder.

Open ```getfiles.py```. Set ```files``` to be the list outputted by the state machine execution.

Run the script using:

```python3 getfiles.py```

In your local ```csvfiles/csvfiles_<timestamp>``` folder, you should see:

- ```claimform_pages_1-1_<TIMESTAMP>.csv```

- ```dischargesummary_pages_2-2_<TIMESTAMP>.csv```

- ```doctorsnote_pages_3-3_<TIMESTAMP>.csv```

- ```tables/```
