# src/deploy/approve_model_package.py
import argparse
import boto3
import os

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--model-package-arn", required=True)
    ap.add_argument("--status", choices=["Approved", "Rejected", "PendingManualApproval"], default="Approved")
    ap.add_argument("--description", default="Approved via CLI")
    args = ap.parse_args()

    sm = boto3.client("sagemaker", region_name=args.region)
    sm.update_model_package(
        ModelPackageArn=args.model_package_arn,
        ModelApprovalStatus=args.status,
        ApprovalDescription=args.description,
    )
    print(f"✅ Updated approval: {args.model_package_arn} -> {args.status}")

if __name__ == "__main__":
    main()