import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";

export class MlopsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ---- Parameters
    const emailForAlerts = new cdk.CfnParameter(this, "EmailForAlerts", {
      type: "String",
      description: "Email address to receive MLOps alerts (SNS subscription).",
    });

    // alerts mode: "failures" or "all"
    const alertsMode = new cdk.CfnParameter(this, "AlertsMode", {
      type: "String",
      description: "Alert mode: 'failures' (only failures) or 'all' (testing: all status changes).",
      allowedValues: ["failures", "all"],
      default: "failures",
    });

    // ---- Artifacts bucket
    const artifactsBucket = new s3.Bucket(this, "MlopsArtifactsBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // ---- SageMaker execution role
    const sagemakerExecutionRole = new iam.Role(this, "SageMakerExecutionRole", {
      assumedBy: new iam.ServicePrincipal("sagemaker.amazonaws.com"),
      description: "Execution role for SageMaker Pipelines / Processing / Training / Registry operations",
    });

    artifactsBucket.grantReadWrite(sagemakerExecutionRole);
    sagemakerExecutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSageMakerFullAccess")
    );

    // ---- Alerts topic
    const alertsTopic = new sns.Topic(this, "MlopsAlertsTopic", {
      displayName: "MLOps Blueprint Alerts",
    });
    alertsTopic.addSubscription(new subs.EmailSubscription(emailForAlerts.valueAsString));

    // ---- Lambda formatter (EventBridge -> Lambda -> SNS publish)
    const formatterFn = new lambda.Function(this, "AlertsFormatterFn", {
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: "handler.main",
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/alerts_formatter")),
      timeout: cdk.Duration.seconds(15),
      environment: {
        TOPIC_ARN: alertsTopic.topicArn,
        ALERTS_MODE: alertsMode.valueAsString, // passed into Lambda too
      },
    });

    alertsTopic.grantPublish(formatterFn);

    const sagemakerSource = ["aws.sagemaker"];

    // ==========================================================
    // Pipeline Execution: ALL statuses (testing mode)
    // ==========================================================
    const pipelineExecutionAllRule = new events.Rule(this, "PipelineExecutionAllRule", {
      description: "SageMaker Pipeline execution status changes (ALL statuses) - testing",
      enabled: alertsMode.valueAsString === "all",
      eventPattern: {
        source: sagemakerSource,
        detailType: ["SageMaker Model Building Pipeline Execution Status Change"],
      },
    });
    pipelineExecutionAllRule.addTarget(new targets.LambdaFunction(formatterFn));

    // Pipeline Execution: FAILED only (always for failures / also ok in testing)
    const pipelineExecutionFailedRule = new events.Rule(this, "PipelineExecutionFailedRule", {
      description: "SageMaker Pipeline execution FAILED",
      eventPattern: {
        source: sagemakerSource,
        detailType: ["SageMaker Model Building Pipeline Execution Status Change"],
        detail: {
          currentPipelineExecutionStatus: ["Failed"],
        },
      },
    });
    pipelineExecutionFailedRule.addTarget(new targets.LambdaFunction(formatterFn));

    // ==========================================================
    // Pipeline Step: ALL statuses (testing mode)
    // ==========================================================
    const pipelineStepAllRule = new events.Rule(this, "PipelineStepAllRule", {
      description: "SageMaker Pipeline step status changes (ALL statuses) - testing",
      enabled: alertsMode.valueAsString === "all",
      eventPattern: {
        source: sagemakerSource,
        detailType: ["SageMaker Model Building Pipeline Execution Step Status Change"],
      },
    });
    pipelineStepAllRule.addTarget(new targets.LambdaFunction(formatterFn));

    // Pipeline Step: FAILED only
    const pipelineStepFailedRule = new events.Rule(this, "PipelineStepFailedRule", {
      description: "SageMaker Pipeline step FAILED",
      eventPattern: {
        source: sagemakerSource,
        detailType: ["SageMaker Model Building Pipeline Execution Step Status Change"],
        detail: {
          stepStatus: ["Failed"],
        },
      },
    });
    pipelineStepFailedRule.addTarget(new targets.LambdaFunction(formatterFn));

    // ==========================================================
    // Model Registry: package state changes (keep always ON)
    // (You can also gate this by alertsMode if you want.)
    // ==========================================================
    const modelPackageRule = new events.Rule(this, "ModelPackageStateChangeRule", {
      description: "SageMaker Model Package State Change (Model Registry)",
      eventPattern: {
        source: sagemakerSource,
        detailType: ["SageMaker Model Package State Change"],
      },
    });
    modelPackageRule.addTarget(new targets.LambdaFunction(formatterFn));

    // ---- Outputs
    new cdk.CfnOutput(this, "ArtifactsBucketName", {
      value: artifactsBucket.bucketName,
      exportName: "ArtifactsBucketName",
    });

    new cdk.CfnOutput(this, "SageMakerExecutionRoleArn", {
      value: sagemakerExecutionRole.roleArn,
      exportName: "SageMakerExecutionRoleArn",
    });

    new cdk.CfnOutput(this, "AlertsTopicArn", {
      value: alertsTopic.topicArn,
      exportName: "AlertsTopicArn",
    });

    new cdk.CfnOutput(this, "AlertsModeOutput", {
    value: alertsMode.valueAsString,
    exportName: "AlertsMode",
});
  }
}