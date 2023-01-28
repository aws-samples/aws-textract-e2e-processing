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

Clone the IDP workflow respository and then ```cd``` into the workflows folder.
```
git clone https://github.com/aws-samples/aws-textract-cdk-commercial-acord.git
cd aws-textract-cdk-commercial-acord/
```

## Document Splitter Workflow Setup

There will be two areas to modify. In our demo today will be using Acord Insurance Forms 

### 1. Comprehend Classifier
If you train your own classifier with a set of documents using workflow1 of this project, you can train your own Comprehend Classifier: 
https://github.com/aws-samples/aws-document-classifier-and-splitter.

After training your classifier and creating an endpoint, you should have a Comprehend Custom Classification Endpoint ARN. 

Navigate to ```docsplitter/document_split_workflow.py``` and modify lines 26-27 which contain ```comprehend_classifier_endpoint```. Paste in your endpoint ARN [in this line](https://github.com/aws-samples/aws-textract-cdk-commercial-acord/blob/main/docsplitter/document_split_workflow.py#L27). It should be in the form: 

```arn:aws:comprehend:<your-region>:<your-account-id>:document-classifier-endpoint/<your-classifier-name>```.


### 2. Document Configuration Table
For each of the document types you trained, you must specify its ```queries``` and ```textract_features```.

```queries```: a list of Queries. For example, "What is the primary email address?" on page 2 of the document. For more information, see the [QueriesConfig Documentation](https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html#Textract-AnalyzeDocument-request-QueriesConfig).

```textract_features```: a list of the Textract features you want to extract from the document. Can one or more of 'TABLES' | 'FORMS' | 'QUERIES' | 'SIGNATURES'.
For more information, see the [FeatureTypes Documentation](https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html#Textract-AnalyzeDocument-request-FeatureTypes).

Navigate to ```lambda/config_prefill/app/generate_csv.py```. Each document type needs its ```classification```, ```queries```, and ```textract_features``` configured, as shown in the examples there by creating ```CSVRow``` instances.

If you want to customize the ```queries``` and/or ```textract_features``` for a specific page in the document, include the page number when configuring its ```CSVRow```.
Otherwise, exclude the page number argument to create a "default" configuration for the specified classification.
Each document type must be configured according to one of the following criteria:
1. Only a default configuration is specified (see the ```acord125``` and ```property_affidavit``` examples in ```generate_csv.py```).
2. A default configuration is specified, in addition to one or more configurations for specific pages (see the ```acord126``` example in ```generate_csv.py```)
3. Each page has a custom configuration--there is not a default configuration (see the ```acord140``` example in ```generate_csv.py```).

For example, if an 8-page document has its first, second, and fourth pages' ```CSVRow``` instances configured, for all of the remaining pages, the "default" configuration will apply.

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
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-10xun1pqvc3j7/textract-csv-output/acord125_pages_1-4_2023-01-05T03:14:28.110714.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-10xun1pqvc3j7/textract-csv-output/acord126_pages_5-8_2023-01-05T03:14:28.146333.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-10xun1pqvc3j7/textract-csv-output/acord140_pages_9-11_2023-01-05T03:14:28.070567.csv"
  },
  {
    "JoinedCSVOutputPath": "s3://documentsplitterworkflow-textractsimplesyncworkfl-10xun1pqvc3j7/textract-csv-output/property_affidavit_pages_12-12_2023-01-05T03:14:28.073459.csv"
  }
]
```

Each of these outputted CSV files is the result of parsing its original pages' Textract AnalyzeDocument output. Each page also includes whether or not it contains a signature.

Open ```getfiles.py```. Set ```files``` to be the list outputted by the state machine execution.

Run the script using:

```python3 getfiles.py```

In the ```csvfiles_<timestamp>``` folder, you should see:

```acord125_pages_1-4_<timestamp>.csv```

```acord126_pages_5-8_<timestamp>.csv```

```acord140_pages_9-11_<timestamp>.csv```

```property_affidavit_pages_12-12_<timestamp>.csv```
