import argparse
import io
import json
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd


# ---------------------------
# Drift metric helpers (PSI)
# ---------------------------

EPS = 1e-6


def _safe_prob(p: float) -> float:
    return max(p, EPS)


def psi_from_distributions(expected: List[float], actual: List[float]) -> float:
    """
    Population Stability Index (PSI)
    PSI = sum((a - e) * ln(a/e)) across bins
    """
    psi = 0.0
    for e, a in zip(expected, actual):
        e2 = _safe_prob(e)
        a2 = _safe_prob(a)
        psi += (a2 - e2) * math.log(a2 / e2)
    return float(psi)


def numeric_bins_from_baseline(series: pd.Series, n_bins: int = 10) -> List[float]:
    """
    Build bin edges using baseline quantiles. Returns sorted edges.
    Uses unique quantiles to avoid duplicate edges on small datasets.
    """
    qs = [i / n_bins for i in range(1, n_bins)]
    edges = series.quantile(qs).dropna().unique().tolist()
    edges = sorted(set(edges))
    return edges


def numeric_distribution(series: pd.Series, edges: List[float]) -> List[float]:
    """
    Compute histogram distribution over bins defined by edges.
    Bins: (-inf, e1], (e1, e2], ... , (ek, +inf)
    """
    s = series.dropna()
    if s.empty:
        # all missing -> put everything in first bin to avoid crash
        return [1.0] + [0.0] * len(edges)

    bins = [-float("inf")] + edges + [float("inf")]
    counts = pd.cut(s, bins=bins, include_lowest=True).value_counts(sort=False)
    dist = (counts / counts.sum()).tolist()
    return [float(x) for x in dist]


def categorical_distribution(series: pd.Series, top_k: int = 20) -> Tuple[List[str], List[float]]:
    """
    Build a stable set of categories using baseline top_k categories (plus '__OTHER__').
    """
    s = series.fillna("__NULL__").astype(str)
    vc = s.value_counts(dropna=False)
    cats = vc.head(top_k).index.tolist()
    # Always include an OTHER bucket
    if "__OTHER__" not in cats:
        cats.append("__OTHER__")
    dist = []
    total = float(len(s))
    for c in cats:
        if c == "__OTHER__":
            in_top = set(cats) - {"__OTHER__"}
            count_other = vc[~vc.index.isin(list(in_top))].sum()
            dist.append(float(count_other) / total if total else 0.0)
        else:
            dist.append(float(vc.get(c, 0)) / total if total else 0.0)
    return cats, dist


def categorical_distribution_on_categories(series: pd.Series, categories: List[str]) -> List[float]:
    s = series.fillna("__NULL__").astype(str)
    vc = s.value_counts(dropna=False)
    total = float(len(s))
    dist = []
    for c in categories:
        if c == "__OTHER__":
            in_top = set(categories) - {"__OTHER__"}
            count_other = vc[~vc.index.isin(list(in_top))].sum()
            dist.append(float(count_other) / total if total else 0.0)
        else:
            dist.append(float(vc.get(c, 0)) / total if total else 0.0)
    return dist


# ---------------------------
# S3 helpers
# ---------------------------

@dataclass
class S3Uri:
    bucket: str
    key: str


def parse_s3_uri(uri: str) -> S3Uri:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    no_scheme = uri.replace("s3://", "", 1)
    parts = no_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    if not bucket:
        raise ValueError(f"Invalid S3 URI (missing bucket): {uri}")
    return S3Uri(bucket=bucket, key=key)


def read_csv_from_s3(s3, uri: str) -> pd.DataFrame:
    loc = parse_s3_uri(uri)
    obj = s3.get_object(Bucket=loc.bucket, Key=loc.key)
    body = obj["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def list_csv_keys_under_prefix(s3, bucket: str, prefix: str, max_files: int = 10) -> List[str]:
    """
    Lists up to max_files CSV objects under a prefix, newest-first by LastModified.
    """
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in page.get("Contents", []):
            k = c["Key"]
            if k.lower().endswith(".csv"):
                keys.append((k, c["LastModified"]))
    keys.sort(key=lambda x: x[1], reverse=True)
    return [k for k, _ in keys[:max_files]]


def read_recent_csvs(s3, prefix_uri: str, max_files: int = 5, max_rows: Optional[int] = None) -> pd.DataFrame:
    loc = parse_s3_uri(prefix_uri)
    keys = list_csv_keys_under_prefix(s3, loc.bucket, loc.key, max_files=max_files)
    if not keys:
        raise FileNotFoundError(f"No CSV files found under prefix: {prefix_uri}")

    frames = []
    for k in keys:
        obj = s3.get_object(Bucket=loc.bucket, Key=k)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    if max_rows and len(out) > max_rows:
        out = out.sample(n=max_rows, random_state=42).reset_index(drop=True)
    return out


# ---------------------------
# CloudWatch + SNS
# ---------------------------

def publish_metrics_to_cloudwatch(
    cw,
    namespace: str,
    dimensions: Dict[str, str],
    metrics: Dict[str, float],
):
    metric_data = []
    for name, value in metrics.items():
        metric_data.append(
            {
                "MetricName": name,
                "Dimensions": [{"Name": k, "Value": v} for k, v in dimensions.items()],
                "Value": float(value),
                "Unit": "None",
            }
        )

    # CloudWatch PutMetricData limit is 20 metrics per call
    for i in range(0, len(metric_data), 20):
        cw.put_metric_data(Namespace=namespace, MetricData=metric_data[i : i + 20])


def maybe_send_sns_alert(sns, topic_arn: Optional[str], subject: str, message: str):
    if not topic_arn:
        return
    sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)


# ---------------------------
# Main
# ---------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--region", default="us-east-1")

    # Data sources
    p.add_argument("--baseline-s3-uri", required=True, help="S3 URI to baseline CSV (single file)")
    p.add_argument("--recent-s3-prefix", required=True, help="S3 URI prefix containing recent CSV files")

    # Optional schema controls
    p.add_argument("--label-col", default="label", help="Column to ignore as label/target")
    p.add_argument("--exclude-cols", default="", help="Comma-separated columns to ignore")
    p.add_argument("--bins", type=int, default=10, help="PSI bins for numeric features")
    p.add_argument("--top-k-cats", type=int, default=20, help="Top K categories for categorical PSI")

    # Sampling controls
    p.add_argument("--recent-max-files", type=int, default=5, help="Max recent CSV files to read")
    p.add_argument("--recent-max-rows", type=int, default=5000, help="Sample down recent rows (0 disables)")

    # Alerting + metrics
    p.add_argument("--psi-threshold", type=float, default=0.25, help="Alert threshold on OverallPSI_Max")
    p.add_argument("--cw-namespace", default="MLOpsBlueprint/Drift")
    p.add_argument("--cw-dimension-name", default="Project", help="CloudWatch dimension name")
    p.add_argument("--cw-dimension-value", default="aws-mlops-blueprint", help="CloudWatch dimension value")
    p.add_argument("--publish-top-feature-metrics", type=int, default=10, help="Publish top N feature PSI metrics")

    p.add_argument("--sns-topic-arn", default="", help="SNS topic ARN to publish alert (optional)")
    args = p.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    cw = boto3.client("cloudwatch", region_name=args.region)
    sns = boto3.client("sns", region_name=args.region)

    exclude = set([c.strip() for c in args.exclude_cols.split(",") if c.strip()])
    exclude.add(args.label_col)

    print("==> Loading baseline:", args.baseline_s3_uri)
    baseline = read_csv_from_s3(s3, args.baseline_s3_uri)

    print("==> Loading recent from prefix:", args.recent_s3_prefix)
    recent_max_rows = None if args.recent_max_rows <= 0 else args.recent_max_rows
    recent = read_recent_csvs(
        s3,
        args.recent_s3_prefix,
        max_files=args.recent_max_files,
        max_rows=recent_max_rows,
    )

    # Align columns (intersection only)
    common_cols = [c for c in baseline.columns if c in recent.columns]
    common_cols = [c for c in common_cols if c not in exclude]
    if not common_cols:
        raise SystemExit(
            f"No comparable columns found. Baseline cols={list(baseline.columns)} recent cols={list(recent.columns)} exclude={exclude}"
        )

    print(f"==> Comparing {len(common_cols)} columns (excluding: {sorted(exclude)})")

    feature_psi: Dict[str, float] = {}

    for col in common_cols:
        b = baseline[col]
        r = recent[col]

        # numeric vs categorical
        if pd.api.types.is_numeric_dtype(b) and pd.api.types.is_numeric_dtype(r):
            edges = numeric_bins_from_baseline(b, n_bins=args.bins)
            e_dist = numeric_distribution(b, edges)
            a_dist = numeric_distribution(r, edges)
            psi = psi_from_distributions(e_dist, a_dist)
            feature_psi[col] = psi
        else:
            cats, e_dist = categorical_distribution(b, top_k=args.top_k_cats)
            a_dist = categorical_distribution_on_categories(r, cats)
            psi = psi_from_distributions(e_dist, a_dist)
            feature_psi[col] = psi

    overall_max = max(feature_psi.values()) if feature_psi else 0.0
    overall_mean = sum(feature_psi.values()) / len(feature_psi) if feature_psi else 0.0

    # Publish CW metrics
    dims = {args.cw_dimension_name: args.cw_dimension_value}
    metrics = {
        "OverallPSI_Max": overall_max,
        "OverallPSI_Mean": overall_mean,
    }

    # Publish top N feature PSI metrics (avoid metric explosion)
    top_n = max(0, int(args.publish_top_feature_metrics))
    if top_n > 0:
        top = sorted(feature_psi.items(), key=lambda x: x[1], reverse=True)[:top_n]
        for name, val in top:
            safe_name = name.replace(" ", "_").replace("/", "_")[:200]
            metrics[f"FeaturePSI_{safe_name}"] = float(val)

    print("==> Publishing CloudWatch metrics:", metrics)
    publish_metrics_to_cloudwatch(cw, args.cw_namespace, dims, metrics)

    # If drift crosses threshold, notify
    if overall_max >= args.psi_threshold:
        subject = f"[DRIFT ALERT] OverallPSI_Max={overall_max:.3f} >= {args.psi_threshold:.3f}"
        msg = {
            "baseline": args.baseline_s3_uri,
            "recent_prefix": args.recent_s3_prefix,
            "overall_max": overall_max,
            "overall_mean": overall_mean,
            "threshold": args.psi_threshold,
            "top_features": sorted(feature_psi.items(), key=lambda x: x[1], reverse=True)[:10],
            "cloudwatch_namespace": args.cw_namespace,
            "dimensions": dims,
        }
        print("==> DRIFT ALERT triggered. Sending SNS (if configured).")
        maybe_send_sns_alert(sns, args.sns_topic_arn or None, subject, json.dumps(msg, indent=2))
    else:
        print(f"==> OK: OverallPSI_Max={overall_max:.3f} < threshold={args.psi_threshold:.3f}")

    print("✅ Drift check complete")


if __name__ == "__main__":
    main()