# src/pipelines/build_pipeline.py

import os
from pathlib import Path

import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.workflow.properties import PropertyFile
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput
from sagemaker.estimator import Estimator
from sagemaker.inputs import TrainingInput
from sagemaker.workflow.functions import Join, JsonGet
from sagemaker.workflow.parameters import ParameterString
from sagemaker.workflow.step_collections import RegisterModel
from sagemaker.model_metrics import MetricsSource, ModelMetrics
from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.pipeline_context import PipelineSession

# This file is /src/pipelines/build_pipeline.py so parents[1] is /src
BASE_DIR = Path(__file__).resolve().parents[1]

# Choose instance types that work for student/free accounts:
# - Processing + training commonly allow ml.t3.medium (quota exists)
# - Model Registry "inference_instances" does NOT accept ml.t3.*; use ml.t2.medium instead
PROCESSING_INSTANCE = "ml.t3.medium"
TRAINING_INSTANCE = "ml.m5.large"
EVAL_INSTANCE = "ml.t3.medium"
INFERENCE_INSTANCE = "ml.t2.medium"
TRANSFORM_INSTANCE = "ml.m5.large"

PIPELINE_NAME_DEFAULT = "mlops-blueprint-pipeline"
MODEL_PACKAGE_GROUP = "mlops-blueprint-model-group"


def get_pipeline(
    region: str,
    role_arn: str,
    default_bucket: str,
    pipeline_name: str = PIPELINE_NAME_DEFAULT,
):
    # Lock region for boto3 under the hood
    os.environ["AWS_REGION"] = region
    os.environ["AWS_DEFAULT_REGION"] = region

    # Standard SM session (for defaults); PipelineSession init varies by SDK
    _ = sagemaker.session.Session(default_bucket=default_bucket)
    pipeline_sess = PipelineSession(default_bucket=default_bucket)

    print(
        f"[build_pipeline] region={region} pipeline_name={pipeline_name} bucket={default_bucket} "
        f"(proc={PROCESSING_INSTANCE}, train={TRAINING_INSTANCE}, eval={EVAL_INSTANCE}, "
        f"infer={INFERENCE_INSTANCE})"
    )

    input_data_uri = ParameterString(
        name="InputDataUri",
        default_value=f"s3://{default_bucket}/data/raw/sample_data.csv",
    )

    # ---------- Processing (preprocess) ----------
    script_processor = ScriptProcessor(
        image_uri=sagemaker.image_uris.retrieve(
            framework="sklearn", region=region, version="1.2-1"
        ),
        command=["python3"],
        role=role_arn,
        instance_type=PROCESSING_INSTANCE,
        instance_count=1,
        sagemaker_session=pipeline_sess,
    )

    processing_step = ProcessingStep(
        name="Preprocess",
        processor=script_processor,
        inputs=[
            ProcessingInput(
                source=input_data_uri,
                destination="/opt/ml/processing/input",
            )
        ],
        outputs=[
            ProcessingOutput(output_name="train", source="/opt/ml/processing/train"),
            ProcessingOutput(output_name="val", source="/opt/ml/processing/val"),
            ProcessingOutput(output_name="test", source="/opt/ml/processing/test"),
        ],
        # IMPORTANT: must be a relative path or S3 URL (NOT Windows absolute path)
        code="src/preprocess/preprocess.py",
        job_arguments=[
            "--input-data", "/opt/ml/processing/input",
            "--output-train", "/opt/ml/processing/train",
            "--output-val", "/opt/ml/processing/val",
            "--output-test", "/opt/ml/processing/test",
        ],
    )

    # ---------- Training ----------
    estimator = Estimator(
        image_uri=sagemaker.image_uris.retrieve(
            framework="sklearn", region=region, version="1.2-1"
        ),
        role=role_arn,
        instance_count=1,
        instance_type=TRAINING_INSTANCE,
        output_path=f"s3://{default_bucket}/artifacts/model",
        sagemaker_session=pipeline_sess,
        # IMPORTANT: relative path
        entry_point="src/train/train.py",
    )

    train_step = TrainingStep(
        name="Train",
        estimator=estimator,
        inputs={
            "train": TrainingInput(
                s3_data=processing_step.properties.ProcessingOutputConfig.Outputs[
                    "train"
                ].S3Output.S3Uri,
                content_type="text/csv",
            ),
            "val": TrainingInput(
                s3_data=processing_step.properties.ProcessingOutputConfig.Outputs[
                    "val"
                ].S3Output.S3Uri,
                content_type="text/csv",
            ),
        },
    )

    # ---------- Evaluation ----------
    eval_processor = ScriptProcessor(
        image_uri=sagemaker.image_uris.retrieve(
            framework="sklearn", region=region, version="1.2-1"
        ),
        command=["python3"],
        role=role_arn,
        instance_type=EVAL_INSTANCE,
        instance_count=1,
        sagemaker_session=pipeline_sess,
    )

    evaluation_report = PropertyFile(
        name="EvaluationReport",
        output_name="evaluation",
        path="evaluation.json",
    )

    eval_step = ProcessingStep(
        name="Evaluate",
        processor=eval_processor,
        inputs=[
            ProcessingInput(
                source=train_step.properties.ModelArtifacts.S3ModelArtifacts,
                destination="/opt/ml/processing/model",
            ),
            ProcessingInput(
                source=processing_step.properties.ProcessingOutputConfig.Outputs[
                    "test"
                ].S3Output.S3Uri,
                destination="/opt/ml/processing/test",
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="evaluation",
                source="/opt/ml/processing/evaluation",
            )
        ],
        # IMPORTANT: relative path
        code="src/evaluate/evaluate.py",
        job_arguments=[
            "--model", "/opt/ml/processing/model",
            "--test", "/opt/ml/processing/test",
            "--output-dir", "/opt/ml/processing/evaluation",
        ],
        property_files=[evaluation_report],
    )

    # ---------- Register (Model Registry) ----------
    # ModelMetrics points at evaluation.json in the evaluation output S3 folder
    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=Join(
                on="/",
                values=[
                    eval_step.properties.ProcessingOutputConfig.Outputs[
                        "evaluation"
                    ].S3Output.S3Uri,
                    "evaluation.json",
                ],
            ),
            content_type="application/json",
        )
    )

    register_step = RegisterModel(
        name="RegisterModel",
        estimator=estimator,
        model_data=train_step.properties.ModelArtifacts.S3ModelArtifacts,
        content_types=["text/csv"],
        response_types=["application/json"],
        # IMPORTANT: ml.t3.* is NOT allowed here (validator rejects it)
        inference_instances=[INFERENCE_INSTANCE],
        transform_instances=[TRANSFORM_INSTANCE],
        model_package_group_name=MODEL_PACKAGE_GROUP,
        approval_status="PendingManualApproval",
        model_metrics=model_metrics,
    )

    # ---------- Condition gate ----------
    # Only register if macro_f1 >= 0.70
    # (If you want first run to always register, set right=0.0 temporarily.)
    cond = ConditionGreaterThanOrEqualTo(
        left=JsonGet(
            step_name=eval_step.name,
            property_file=evaluation_report,
            json_path="macro_f1",
        ),
        right=0.70,
    )

    condition_step = ConditionStep(
        name="CheckMetrics",
        conditions=[cond],
        if_steps=[register_step],
        else_steps=[],
    )

    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[input_data_uri],
        steps=[processing_step, train_step, eval_step, condition_step],
        sagemaker_session=pipeline_sess,
    )
    return pipeline


if __name__ == "__main__":
    region = os.environ.get("AWS_REGION", "us-east-1")
    role_arn = os.environ["SAGEMAKER_ROLE_ARN"]
    bucket = os.environ["ARTIFACT_BUCKET"]

    p = get_pipeline(region=region, role_arn=role_arn, default_bucket=bucket)
    p.upsert(role_arn=role_arn)
    print(f"✅ Upserted pipeline: {p.name}")