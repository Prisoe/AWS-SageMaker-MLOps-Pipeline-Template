import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";

export class MlopsStack extends cdk.Stack {
  public readonly artifactBucket: s3.Bucket;
  public readonly sagemakerExecutionRole: iam.Role;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 bucket for datasets + pipeline artifacts + model artifacts
    this.artifactBucket = new s3.Bucket(this, "MlopsArtifactsBucket", {
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // dev-friendly; change to RETAIN for prod
      autoDeleteObjects: true, // dev-friendly; remove for prod
    });

    // SageMaker execution role (start broader, tighten later)
    this.sagemakerExecutionRole = new iam.Role(this, "SageMakerExecutionRole", {
      assumedBy: new iam.ServicePrincipal("sagemaker.amazonaws.com"),
      description: "Execution role for SageMaker Pipelines / Training / Hosting",
    });

    // Minimal permissions:
    // 1) Allow SageMaker to read/write to THIS bucket
    this.artifactBucket.grantReadWrite(this.sagemakerExecutionRole);

    // 2) Allow CloudWatch logging (for training jobs / processing jobs)
    this.sagemakerExecutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("CloudWatchLogsFullAccess")
    );

    // 3) Allow SageMaker full access for now (OK for template dev; tighten later)
    // If your org/account blocks this, we can replace with least-privilege policies.
    this.sagemakerExecutionRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSageMakerFullAccess")
    );

    // Outputs (so you can copy/paste values later)
    new cdk.CfnOutput(this, "ArtifactsBucketName", {
      value: this.artifactBucket.bucketName,
    });

    new cdk.CfnOutput(this, "SageMakerRoleArn", {
      value: this.sagemakerExecutionRole.roleArn,
    });
  }
}