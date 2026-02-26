import os
import boto3

def main():
    sm = boto3.client("sagemaker", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    pipeline_name = "mlops-blueprint-pipeline"

    resp = sm.start_pipeline_execution(
        PipelineName=pipeline_name,
        PipelineExecutionDisplayName="first-run"
    )
    print("✅ Started execution:", resp["PipelineExecutionArn"])

if __name__ == "__main__":
    main()