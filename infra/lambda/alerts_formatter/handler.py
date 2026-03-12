import json
import os
from datetime import datetime, timezone

import boto3

TOPIC_ARN = os.environ["TOPIC_ARN"]
ALERTS_MODE = os.environ.get("ALERTS_MODE", "failures").lower()

sns = boto3.client("sns")


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _get(d: dict, path: str, default=""):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _first_non_empty(*vals):
    for v in vals:
        if v is not None and str(v).strip() != "":
            return v
    return ""


def _format_pipeline_exec(event: dict):
    # AWS can differ: some use pipelineArn/pipelineExecutionArn, others PipelineArn etc.
    pipeline_arn = _first_non_empty(
        _get(event, "detail.pipelineArn", ""),
        _get(event, "detail.PipelineArn", ""),
        _get(event, "resources.0", ""),
    )

    exec_arn = _first_non_empty(
        _get(event, "detail.pipelineExecutionArn", ""),
        _get(event, "detail.PipelineExecutionArn", ""),
    )

    status = _first_non_empty(
        _get(event, "detail.currentPipelineExecutionStatus", ""),
        _get(event, "detail.PipelineExecutionStatus", ""),
        _get(event, "detail.status", ""),
    )

    reason = _first_non_empty(
        _get(event, "detail.failureReason", ""),
        _get(event, "detail.FailureReason", ""),
        "",
    )

    return pipeline_arn, exec_arn, status, reason


def _format_pipeline_step(event: dict):
    pipeline_arn = _first_non_empty(
        _get(event, "detail.pipelineArn", ""),
        _get(event, "detail.PipelineArn", ""),
        "",
    )
    exec_arn = _first_non_empty(
        _get(event, "detail.pipelineExecutionArn", ""),
        _get(event, "detail.PipelineExecutionArn", ""),
        "",
    )
    step_name = _first_non_empty(
        _get(event, "detail.stepName", ""),
        _get(event, "detail.StepName", ""),
        "",
    )
    step_status = _first_non_empty(
        _get(event, "detail.stepStatus", ""),
        _get(event, "detail.StepStatus", ""),
        "",
    )
    reason = _first_non_empty(
        _get(event, "detail.failureReason", ""),
        _get(event, "detail.FailureReason", ""),
        "",
    )

    return pipeline_arn, exec_arn, step_name, step_status, reason


def _format_model_package(event: dict):
    group = _first_non_empty(_get(event, "detail.ModelPackageGroupName", ""), "")
    version = _first_non_empty(_get(event, "detail.ModelPackageVersion", ""), "")
    status = _first_non_empty(_get(event, "detail.ModelPackageStatus", ""), "")
    approval = _first_non_empty(_get(event, "detail.ModelApprovalStatus", ""), "")
    arn = _first_non_empty(_get(event, "detail.ModelPackageArn", ""), "")
    return group, version, status, approval, arn


def _format(event: dict):
    detail_type = event.get("detail-type", "")
    region = event.get("region", "")
    account = event.get("account", "")
    time_utc = event.get("time", _utc_now())

    header = [
        "MLOps Blueprint Alert",
        "====================",
        f"Mode: {ALERTS_MODE}",
        f"Type: {detail_type}",
        f"Time (UTC): {time_utc}",
        f"Region: {region}",
        f"Account: {account}",
        "",
    ]

    # Pipeline execution status change
    if "Pipeline Execution Status Change" in detail_type:
        pipeline_arn, exec_arn, status, reason = _format_pipeline_exec(event)

        emoji = "✅"
        if str(status).lower() in ("failed",):
            emoji = "❌"
        elif str(status).lower() in ("stopped", "stopping"):
            emoji = "🛑"
        elif str(status).lower() in ("executing", "running"):
            emoji = "⏳"

        subject = f"{emoji} Pipeline {status or 'StatusChange'}"
        body = header + [
            "Pipeline Execution",
            "------------------",
            f"PipelineArn: {pipeline_arn}",
            f"ExecutionArn: {exec_arn}",
            f"Status: {status}",
        ]
        if reason:
            body.append(f"FailureReason: {reason}")

        return subject, "\n".join(body)

    # Pipeline step status change
    if "Pipeline Execution Step Status Change" in detail_type:
        pipeline_arn, exec_arn, step_name, step_status, reason = _format_pipeline_step(event)

        emoji = "✅"
        if str(step_status).lower() in ("failed",):
            emoji = "❌"
        elif str(step_status).lower() in ("executing", "running"):
            emoji = "⏳"

        subject = f"{emoji} Step {step_status or 'StatusChange'} ({step_name or 'unknown-step'})"
        body = header + [
            "Pipeline Step",
            "-------------",
            f"PipelineArn: {pipeline_arn}",
            f"ExecutionArn: {exec_arn}",
            f"StepName: {step_name}",
            f"StepStatus: {step_status}",
        ]
        if reason:
            body.append(f"FailureReason: {reason}")

        return subject, "\n".join(body)

    # Model package state change
    if "Model Package State Change" in detail_type:
        group, version, status, approval, arn = _format_model_package(event)

        subject = f"📦 Model Registry v{version} ({approval})"
        body = header + [
            "Model Registry",
            "--------------",
            f"Group: {group}",
            f"Version: {version}",
            f"Status: {status}",
            f"Approval: {approval}",
            f"Arn: {arn}",
        ]
        return subject, "\n".join(body)

    # Fallback
    subject = f"ℹ️ MLOps Alert ({detail_type})"
    body = header + [
        "Raw event (truncated):",
        json.dumps(event, indent=2)[:3500],
    ]
    return subject, "\n".join(body)


def main(event, context):
    subject, message = _format(event)

    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=subject[:100],
        Message=message,
    )

    return {"ok": True}