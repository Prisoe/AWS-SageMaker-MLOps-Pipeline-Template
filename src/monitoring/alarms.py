import argparse
import boto3


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--sns-topic-arn", required=True)

    p.add_argument("--namespace", default="MLOpsBlueprint/Drift")
    p.add_argument("--metric-name", default="OverallPSI_Max")

    p.add_argument("--dimension-name", default="Project")
    p.add_argument("--dimension-value", default="aws-mlops-blueprint")

    p.add_argument("--threshold", type=float, default=0.25)
    p.add_argument("--period", type=int, default=300)  # 5 min
    p.add_argument("--evaluation-periods", type=int, default=1)
    p.add_argument("--datapoints-to-alarm", type=int, default=1)

    p.add_argument("--alarm-name", default="mlops-blueprint-drift-alarm")
    args = p.parse_args()

    cw = boto3.client("cloudwatch", region_name=args.region)

    cw.put_metric_alarm(
        AlarmName=args.alarm_name,
        AlarmDescription=(
            f"Triggers when {args.namespace}/{args.metric_name} >= {args.threshold} "
            f"for {args.datapoints_to_alarm}/{args.evaluation_periods} periods."
        ),
        Namespace=args.namespace,
        MetricName=args.metric_name,
        Dimensions=[{"Name": args.dimension_name, "Value": args.dimension_value}],
        Statistic="Maximum",
        Period=args.period,
        EvaluationPeriods=args.evaluation_periods,
        DatapointsToAlarm=args.datapoints_to_alarm,
        Threshold=args.threshold,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[args.sns_topic_arn],
        OKActions=[args.sns_topic_arn],
    )

    print("✅ CloudWatch alarm upserted:", args.alarm_name)
    print("Namespace/Metric:", args.namespace, args.metric_name)
    print("Dimension:", args.dimension_name, "=", args.dimension_value)
    print("Threshold:", args.threshold)


if __name__ == "__main__":
    main()