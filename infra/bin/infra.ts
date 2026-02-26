#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { MlopsStack } from "../lib/mlops-stack";

const app = new cdk.App();

new MlopsStack(app, "MlopsBlueprintStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});