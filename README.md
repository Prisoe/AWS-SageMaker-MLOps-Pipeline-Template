# AWS SageMaker MLOps Blueprint  
**SageMaker Pipelines → Evaluation Gate → Model Registry**

**Goal:** Demonstrate an end-to-end MLOps workflow on AWS using **SageMaker Pipelines**:

- Raw data in S3 → preprocessing/splitting → training → evaluation  
- A **quality gate** (ConditionStep) blocks/permits model promotion  
- Passing models are **registered** in **SageMaker Model Registry** (Model Package Group)

🎥 **Video demo:** https://youtu.be/IdLPluh7-eo

---

## Why this project matters (portfolio summary)

This project shows that I can build and operationalize a machine learning workflow on AWS, including orchestration, governance, and debugging cloud constraints.

**Highlights**
- Built an end-to-end SageMaker Pipeline using the SageMaker Python SDK
- Implemented:
  - **ProcessingStep** (preprocess + train/val/test split)
  - **TrainingStep** (scikit-learn training job)
  - **ProcessingStep** (evaluation + metrics artifact)
  - **ConditionStep** (metric gate using macro F1)
  - **RegisterModel** (Model Registry versioning + metadata)
- Debugged real-world constraints:
  - SageMaker quotas / instance availability (student/free account constraints)
  - Model artifact format differences (`model.tar.gz` vs `model.joblib`)
  - CloudWatch log-driven troubleshooting to root-cause failures

---

## Architecture

### Pipeline flow (high level)

```text
S3 Raw Data
  │
  ▼
Preprocess (ProcessingStep)
  - clean/split to train/val/test
  - outputs: S3 prefixes for train/val/test
  │
  ▼
Train (TrainingStep)
  - trains sklearn model
  - output: model.tar.gz in S3
  │
  ▼
Evaluate (ProcessingStep)
  - extracts model.tar.gz
  - computes macro_f1 + classification_report
  - output: evaluation.json in S3
  │
  ▼
CheckMetrics (ConditionStep)
  - if macro_f1 >= threshold → RegisterModel
  - else stop
  │
  ▼
RegisterModel (Model Registry)
  - creates model package version
  - attaches metrics + artifacts
  - approval status: PendingManualApproval


Orchestration (defines the DAG):
  src/pipelines/build_pipeline.py
  src/pipelines/run_pipeline.py

Workload (executes inside AWS jobs):
  src/preprocess/preprocess.py
  src/train/train.py
  src/evaluate/evaluate.py




aws-mlops-blueprint/
├── infra/                          # CDK infra (IAM role, S3 artifacts bucket, etc.)
├── ml/
│   └── sample_data.csv             # Example dataset uploaded to S3
└── src/
    ├── pipelines/
    │   ├── build_pipeline.py       # defines + upserts SageMaker Pipeline (DAG)
    │   └── run_pipeline.py         # starts a pipeline execution
    ├── preprocess/
    │   └── preprocess.py           # raw CSV → train/val/test splits
    ├── train/
    │   └── train.py                # trains RandomForest → SM_MODEL_DIR
    └── evaluate/
        └── evaluate.py             # extracts model.tar.gz → evaluates → evaluation.json





- Prerequisites
Confirm AWS CLI is configured:
aws sts get-caller-identity


- Requirements:
Python (recommended: 3.10+)
Node.js + AWS CDK (only if you deploy infra from infra/)


- How to run (Windows PowerShell)
These are the exact steps/commands used during development.

1) Create + activate venv
python -m venv .venv1
.\.venv1\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
pip install "sagemaker>=3.0.0" boto3 pandas scikit-learn joblib


2) Deploy infrastructure (CDK)
cd infra
npm install
cdk bootstrap
cdk deploy
cd ..

This should create:
An S3 artifacts bucket
A SageMaker execution role


3) Upload data to S3
aws s3 cp ml/sample_data.csv s3://<ARTIFACT_BUCKET>/data/raw/sample_data.csv


4) Set environment variables
$env:SAGEMAKER_ROLE_ARN="arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>"
$env:ARTIFACT_BUCKET="<ARTIFACT_BUCKET>"
$env:AWS_REGION="us-east-1"


5) Build (upsert) the pipeline
python -u .\src\pipelines\build_pipeline.py
aws sagemaker list-pipelines --region us-east-1


6) Run the pipeline
python -u .\src\pipelines\run_pipeline.py
Monitoring & debugging
- Check executions
aws sagemaker list-pipeline-executions `
  --pipeline-name mlops-blueprint-pipeline `
  --region us-east-1
- Check execution steps (most important)
aws sagemaker list-pipeline-execution-steps `
  --pipeline-execution-arn "<PIPELINE_EXECUTION_ARN>" `
  --region us-east-1
  
  
- Pull CloudWatch logs for failures
- Processing jobs:

aws logs describe-log-streams `
  --log-group-name "/aws/sagemaker/ProcessingJobs" `
  --order-by LastEventTime `
  --descending `
  --max-items 5 `
  --region us-east-1

aws logs get-log-events `
  --log-group-name "/aws/sagemaker/ProcessingJobs" `
  --log-stream-name "<LOG_STREAM_NAME>" `
  --limit 200 `
  --region us-east-1

- Training jobs:

aws logs describe-log-streams `
  --log-group-name "/aws/sagemaker/TrainingJobs" `
  --order-by LastEventTime `
  --descending `
  --max-items 5 `
  --region us-east-1

aws logs get-log-events `
  --log-group-name "/aws/sagemaker/TrainingJobs" `
  --log-stream-name "<LOG_STREAM_NAME>" `
  --limit 200 `
  --region us-east-1
  
  
  
- Successful run proof (Pipeline execution)


{
  "PipelineExecutionSteps": [
    { "StepName": "Preprocess", "StepStatus": "Succeeded" },
    { "StepName": "Train", "StepStatus": "Succeeded" },
    { "StepName": "Evaluate", "StepStatus": "Succeeded" },
    { "StepName": "CheckMetrics", "StepStatus": "Succeeded", "Outcome": "True" },
    { "StepName": "RegisterModel-RegisterModel", "StepStatus": "Succeeded" }
  ]
}