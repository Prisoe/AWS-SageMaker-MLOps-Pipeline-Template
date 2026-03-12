import os
import boto3
import datetime

sm = boto3.client("sagemaker")

def handler(event, context):
    pipeline_name = os.environ["PIPELINE_NAME"]
    display_name = os.environ.get("EXECUTION_DISPLAY_NAME", "scheduled-run")
    # include timestamp so every execution is unique
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    exec_name = f"{display_name}-{ts}"

    resp = sm.start_pipeline_execution(
        PipelineName=pipeline_name,
        PipelineExecutionDisplayName=exec_name,
    )
    return {
        "pipeline": pipeline_name,
        "executionArn": resp["PipelineExecutionArn"],
        "displayName": exec_name
    }