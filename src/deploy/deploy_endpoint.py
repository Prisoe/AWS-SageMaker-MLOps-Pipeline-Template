# src/deploy/deploy_endpoint.py
import argparse
import io
import json
import os
import tarfile
import time
from datetime import datetime, timezone

import boto3


DEFAULT_PROJECT = "aws-mlops-blueprint"
DEFAULT_MODEL_PKG_GROUP = "mlops-blueprint-model-group"


def _now_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _get_latest_model_package_arn(sm, model_package_group: str, approval_status: str = "Approved") -> str:
    resp = sm.list_model_packages(
        ModelPackageGroupName=model_package_group,
        SortBy="CreationTime",
        SortOrder="Descending",
        MaxResults=50,
    )
    pkgs = resp.get("ModelPackageSummaryList", [])
    for p in pkgs:
        if p.get("ModelPackageStatus") == "Completed" and p.get("ModelApprovalStatus") == approval_status:
            return p["ModelPackageArn"]

    raise RuntimeError(
        f"No model packages found with status=Completed and approval={approval_status} "
        f"in group '{model_package_group}'."
    )


def _build_inference_tarball_bytes() -> bytes:
    """
    Create a minimal SageMaker SKLearn Script Mode inference module.
    This will be extracted by the container and imported using SAGEMAKER_PROGRAM.
    """
    inference_py = r'''
import json
import os
import joblib
import numpy as np

MODEL_PATH = "/opt/ml/model/model.joblib"

def model_fn(model_dir):
    # model_dir is typically /opt/ml/model
    path = os.path.join(model_dir, "model.joblib")
    return joblib.load(path)

def input_fn(request_body, request_content_type):
    # Accept CSV or JSON list
    if request_content_type == "text/csv":
        rows = [r.strip() for r in request_body.strip().splitlines() if r.strip()]
        data = [[float(x) for x in row.split(",")] for row in rows]
        return np.array(data, dtype=float)

    if request_content_type == "application/json":
        obj = json.loads(request_body)
        # allow {"instances": [[...], ...]} or [[...], ...]
        if isinstance(obj, dict) and "instances" in obj:
            obj = obj["instances"]
        return np.array(obj, dtype=float)

    raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(input_data, model):
    preds = model.predict(input_data)
    return preds

def output_fn(prediction, accept):
    if accept == "application/json" or accept is None:
        return json.dumps({"predictions": prediction.tolist()}), "application/json"
    # fallback to text
    return "\n".join(str(x) for x in prediction.tolist()), "text/plain"
'''.lstrip()

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        content = inference_py.encode("utf-8")
        info = tarfile.TarInfo(name="inference.py")
        info.size = len(content)
        tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def _upload_inference_bundle(s3, bucket: str, key: str) -> str:
    data = _build_inference_tarball_bytes()
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    return f"s3://{bucket}/{key}"


def _safe_create_or_update_model(
    sm,
    model_name: str,
    role_arn: str,
    image: str,
    model_data_url: str,
    code_s3_uri: str,
    entry_point: str = "inference.py",
):
    # If model exists, delete and recreate (simple & deterministic for demos)
    try:
        sm.describe_model(ModelName=model_name)
        print(f"==> Model exists, deleting: {model_name}")
        sm.delete_model(ModelName=model_name)
        time.sleep(2)
    except sm.exceptions.ClientError:
        pass

    print(f"==> Creating model: {model_name}")
    sm.create_model(
        ModelName=model_name,
        ExecutionRoleArn=role_arn,
        PrimaryContainer={
            "Image": image,
            "ModelDataUrl": model_data_url,
            "Environment": {
                # Script Mode inference
                "SAGEMAKER_PROGRAM": entry_point,
                "SAGEMAKER_SUBMIT_DIRECTORY": code_s3_uri,
                # Optional but nice
                "SAGEMAKER_REGION": os.environ.get("AWS_REGION", "us-east-1"),
                "SAGEMAKER_CONTAINER_LOG_LEVEL": "20",
            },
        },
        EnableNetworkIsolation=False,
    )


def _safe_create_or_update_endpoint_config(
    sm,
    endpoint_config_name: str,
    model_name: str,
    instance_type: str,
    initial_instance_count: int,
    data_capture_s3_uri: str,
):
    try:
        sm.describe_endpoint_config(EndpointConfigName=endpoint_config_name)
        print(f"==> EndpointConfig exists, deleting: {endpoint_config_name}")
        sm.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
        time.sleep(2)
    except sm.exceptions.ClientError:
        pass

    print(f"==> Creating EndpointConfig: {endpoint_config_name}")
    sm.create_endpoint_config(
        EndpointConfigName=endpoint_config_name,
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": model_name,
                "InitialInstanceCount": initial_instance_count,
                "InstanceType": instance_type,
                "InitialVariantWeight": 1.0,
            }
        ],
        DataCaptureConfig={
            "EnableCapture": True,
            "InitialSamplingPercentage": 100,
            "DestinationS3Uri": data_capture_s3_uri,
            "CaptureOptions": [{"CaptureMode": "Input"}, {"CaptureMode": "Output"}],
            "CaptureContentTypeHeader": {"CsvContentTypes": ["text/csv"], "JsonContentTypes": ["application/json"]},
        },
    )


def _safe_create_or_update_endpoint(sm, endpoint_name: str, endpoint_config_name: str, delete_failed: bool = False):
    try:
        desc = sm.describe_endpoint(EndpointName=endpoint_name)
        status = desc.get("EndpointStatus")
        arn = desc.get("EndpointArn")
        print(f"==> Endpoint exists: status={status} arn={arn}")

        if status == "Failed" and delete_failed:
            print(f"==> Endpoint is Failed. Deleting endpoint: {endpoint_name}")
            sm.delete_endpoint(EndpointName=endpoint_name)
            # wait until it's gone
            for _ in range(60):
                time.sleep(10)
                try:
                    sm.describe_endpoint(EndpointName=endpoint_name)
                except sm.exceptions.ClientError:
                    print("==> Endpoint deleted.")
                    break

        else:
            print(f"==> Updating endpoint to config: {endpoint_config_name}")
            sm.update_endpoint(EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name)
            return

    except sm.exceptions.ClientError:
        pass

    print(f"==> Creating endpoint: {endpoint_name}")
    sm.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name)


def _wait_for_endpoint_in_service(sm, endpoint_name: str, poll_seconds: int = 30, timeout_minutes: int = 40):
    deadline = time.time() + timeout_minutes * 60
    while True:
        desc = sm.describe_endpoint(EndpointName=endpoint_name)
        status = desc.get("EndpointStatus")
        print(f"==> EndpointStatus: {status}")
        if status == "InService":
            return desc
        if status in ("Failed", "OutOfService"):
            raise RuntimeError(f"Endpoint deployment failed: {json.dumps(desc, default=str)[:2000]}")
        if time.time() > deadline:
            raise TimeoutError(f"Timed out waiting for InService after {timeout_minutes} minutes.")
        time.sleep(poll_seconds)


def main():
    ap = argparse.ArgumentParser(description="Deploy a SageMaker Model Package to a realtime endpoint (with Script Mode inference).")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--role-arn", default=os.environ.get("SAGEMAKER_ROLE_ARN"))
    ap.add_argument("--artifact-bucket", default=os.environ.get("ARTIFACT_BUCKET"))

    ap.add_argument("--model-package-group", default=DEFAULT_MODEL_PKG_GROUP)
    ap.add_argument("--model-package-arn", default=None)
    ap.add_argument("--allow-pending", action="store_true")

    ap.add_argument("--endpoint-name", default=f"{DEFAULT_PROJECT}-endpoint")
    ap.add_argument("--instance-type", default="ml.t2.medium")
    ap.add_argument("--initial-instance-count", type=int, default=1)

    ap.add_argument("--delete-failed-endpoint", action="store_true")
    ap.add_argument("--wait", action="store_true")
    args = ap.parse_args()

    if not args.role_arn:
        raise SystemExit("Missing --role-arn (or env SAGEMAKER_ROLE_ARN).")
    if not args.artifact_bucket:
        raise SystemExit("Missing --artifact-bucket (or env ARTIFACT_BUCKET).")

    sm = boto3.client("sagemaker", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    # pick model package
    if args.model_package_arn:
        model_pkg_arn = args.model_package_arn
    else:
        model_pkg_arn = _get_latest_model_package_arn(sm, args.model_package_group, approval_status="Approved")

    mp = sm.describe_model_package(ModelPackageName=model_pkg_arn)
    approval = mp.get("ModelApprovalStatus")
    status = mp.get("ModelPackageStatus")

    print(f"==> Using ModelPackageArn: {model_pkg_arn}")
    print(f"==> Package status={status}, approval={approval}")

    if status != "Completed":
        raise SystemExit("Model package is not Completed yet.")
    if approval != "Approved" and not args.allow_pending:
        raise SystemExit("Model package is not Approved. Approve it or rerun with --allow-pending.")

    # get image + model artifacts from the model package
    container0 = mp["InferenceSpecification"]["Containers"][0]
    image = container0["Image"]
    model_data_url = container0["ModelDataUrl"]
    print(f"==> Image: {image}")
    print(f"==> ModelDataUrl: {model_data_url}")

    # upload inference bundle (tar.gz) to S3
    code_key = f"artifacts/inference/code-{_now_suffix()}.tar.gz"
    code_s3_uri = _upload_inference_bundle(s3, args.artifact_bucket, code_key)
    print(f"==> Uploaded inference code: {code_s3_uri}")

    # names
    suffix = _now_suffix()
    model_name = f"{DEFAULT_PROJECT}-model-{suffix}"
    endpoint_config_name = f"{DEFAULT_PROJECT}-cfg-{suffix}"

    data_capture_s3_uri = f"s3://{args.artifact_bucket}/monitoring/data-capture/{args.endpoint_name}/"

    # create model using Image+ModelDataUrl + Script Mode env
    _safe_create_or_update_model(
        sm=sm,
        model_name=model_name,
        role_arn=args.role_arn,
        image=image,
        model_data_url=model_data_url,
        code_s3_uri=code_s3_uri,
        entry_point="inference.py",
    )

    _safe_create_or_update_endpoint_config(
        sm=sm,
        endpoint_config_name=endpoint_config_name,
        model_name=model_name,
        instance_type=args.instance_type,
        initial_instance_count=args.initial_instance_count,
        data_capture_s3_uri=data_capture_s3_uri,
    )

    _safe_create_or_update_endpoint(
        sm=sm,
        endpoint_name=args.endpoint_name,
        endpoint_config_name=endpoint_config_name,
        delete_failed=args.delete_failed_endpoint,
    )

    print("✅ Deployment request submitted.")
    print(f"EndpointName: {args.endpoint_name}")
    print(f"EndpointConfigName: {endpoint_config_name}")
    print(f"ModelName: {model_name}")
    print(f"DataCapture S3: {data_capture_s3_uri}")

    if args.wait:
        final = _wait_for_endpoint_in_service(sm, args.endpoint_name)
        print("✅ Endpoint is InService.")
        print(json.dumps(final, default=str, indent=2))


if __name__ == "__main__":
    main()