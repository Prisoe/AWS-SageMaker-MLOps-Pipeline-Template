param(
  [Parameter(Mandatory=$false)][string]$Region = "us-east-1",
  [Parameter(Mandatory=$false)][string]$StackName = "MlopsBlueprintStack",
  [Parameter(Mandatory=$false)][string]$EndpointName = "aws-mlops-blueprint-endpoint",
  [Parameter(Mandatory=$false)][string]$InstanceType = "ml.t2.medium",
  [Parameter(Mandatory=$false)][int]$InitialInstanceCount = 1,
  [Parameter(Mandatory=$false)][string]$ModelPackageArn = "",
  [Parameter(Mandatory=$false)][switch]$AllowPending,
  [Parameter(Mandatory=$false)][switch]$Wait,
  [Parameter(Mandatory=$false)][switch]$DeleteFailedEndpoint
)

$ErrorActionPreference = "Stop"

function Exec([string]$FilePath, [object[]]$CmdArgs) {
  & $FilePath @CmdArgs
  if ($LASTEXITCODE -ne 0) {
    throw "Command failed (exit=$LASTEXITCODE): $FilePath $($CmdArgs -join ' ')"
  }
}

# Expect you already activated .venv1
$Py = (Get-Command python).Source
Write-Host "==> Using python: $Py"

# Pull stack outputs (RoleArn + Bucket)
Write-Host "`n==> Reading stack outputs"
$RoleArn = aws cloudformation describe-stacks `
  --stack-name $StackName `
  --region $Region `
  --query "Stacks[0].Outputs[?OutputKey=='SageMakerExecutionRoleArn'].OutputValue" `
  --output text

$Bucket = aws cloudformation describe-stacks `
  --stack-name $StackName `
  --region $Region `
  --query "Stacks[0].Outputs[?OutputKey=='ArtifactsBucketName'].OutputValue" `
  --output text

if (-not $RoleArn -or -not $Bucket) {
  throw "Missing outputs from stack. Need SageMakerExecutionRoleArn and ArtifactsBucketName."
}

Write-Host "RoleArn: $RoleArn"
Write-Host "Bucket: $Bucket"

$env:AWS_REGION = $Region
$env:AWS_DEFAULT_REGION = $Region
$env:SAGEMAKER_ROLE_ARN = $RoleArn
$env:ARTIFACT_BUCKET = $Bucket

$DeployScript = Join-Path (Get-Location) "src\deploy\deploy_endpoint.py"
if (!(Test-Path $DeployScript)) {
  throw "Missing deploy script: $DeployScript"
}

$cmd = @(
  $DeployScript,
  "--region", $Region,
  "--endpoint-name", $EndpointName,
  "--instance-type", $InstanceType,
  "--initial-instance-count", "$InitialInstanceCount"
)

if ($ModelPackageArn -and $ModelPackageArn.Trim().Length -gt 0) {
  $cmd += @("--model-package-arn", $ModelPackageArn)
}
if ($DeleteFailedEndpoint) { $cmd += @("--delete-failed-endpoint") }

if ($AllowPending) { $cmd += @("--allow-pending") }
if ($Wait) { $cmd += @("--wait") }

Write-Host "`n==> Deploying endpoint"
Exec $Py $cmd

Write-Host "`n✅ Done."