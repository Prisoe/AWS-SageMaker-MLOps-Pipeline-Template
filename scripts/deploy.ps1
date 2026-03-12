param(
  [Parameter(Mandatory=$false)][string]$Region = "us-east-1",
  [Parameter(Mandatory=$true)][string]$EmailForAlerts,
  [Parameter(Mandatory=$false)][string]$AlertsMode = "failures",   # failures | all
  [Parameter(Mandatory=$false)][string]$StackName = "MlopsBlueprintStack",
  [Parameter(Mandatory=$false)][string]$PipelineName = "mlops-blueprint-pipeline",
  [Parameter(Mandatory=$false)][string]$VenvDir = ".venv1"
)

$ErrorActionPreference = "Stop"

function Exec {
  param(
    [Parameter(Mandatory=$true)][string]$FilePath,
    [Parameter(Mandatory=$true)][object[]]$Args,
    [Parameter(Mandatory=$false)][string]$WorkingDir = $null
  )

  if (-not $Args -or $Args.Count -eq 0) {
    throw "Refusing to run '$FilePath' with empty args (would open interactive shell)."
  }

  if ($WorkingDir) { Push-Location $WorkingDir }
  try {
    Write-Host "==> $FilePath $($Args -join ' ')" -ForegroundColor DarkGray
    & $FilePath @Args
    if ($LASTEXITCODE -ne 0) {
      throw "Command failed (exit=$LASTEXITCODE): $FilePath $($Args -join ' ')"
    }
  } finally {
    if ($WorkingDir) { Pop-Location }
  }
}

# --- Repo root (scripts folder is /scripts) ---
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Write-Host "`n==> Repo root: $RepoRoot`n"

# --- Python / venv ---
$VenvPath = Join-Path $RepoRoot $VenvDir
$Py = Join-Path $VenvPath "Scripts\python.exe"

if (!(Test-Path $Py)) {
  Write-Host "==> Creating venv at $VenvPath"
  Exec -FilePath "python" -Args @("-m","venv",$VenvPath)
}

Write-Host "==> Using venv python: $Py"
Exec -FilePath $Py -Args @("-c","import sys; print(sys.executable); print(sys.version)")

# --- Sanity checks ---
Write-Host "`n==> Sanity checks"
Exec -FilePath "aws" -Args @("sts","get-caller-identity","--region",$Region)

# --- Install pinned Python deps (CRITICAL: do NOT install sagemaker==3.x) ---
Write-Host "`n==> Ensuring Python dependencies in venv (PINNED)"
Exec -FilePath $Py -Args @("-m","pip","install","--upgrade","pip","setuptools","wheel")

$ReqFile = Join-Path $RepoRoot "requirements.txt"
if (!(Test-Path $ReqFile)) {
  throw "Missing requirements.txt at repo root. Create it and pin sagemaker==2.232.0."
}

$reqText = Get-Content $ReqFile -Raw
if ($reqText -match "sagemaker==3\." ) {
  throw "requirements.txt pins sagemaker==3.x. Pipeline code needs sagemaker.workflow -> pin sagemaker==2.232.0."
}

Exec -FilePath $Py -Args @("-m","pip","install","--no-cache-dir","-r",$ReqFile)

# Verify workflow module exists
Exec -FilePath $Py -Args @("-c","import sagemaker; import sagemaker.workflow; print('OK workflow:', sagemaker.__file__)")

# --- Deploy CDK infrastructure ---
Write-Host "`n==> Deploying CDK infrastructure"
$InfraDir = Join-Path $RepoRoot "infra"
if (!(Test-Path $InfraDir)) { throw "infra/ folder not found at $InfraDir" }

Exec -FilePath "npm" -Args @("install") -WorkingDir $InfraDir

$AccountId = (aws sts get-caller-identity --query Account --output text --region $Region)
Exec -FilePath "npx" -Args @("cdk","bootstrap","aws://$AccountId/$Region") -WorkingDir $InfraDir

$OutputsFile = Join-Path $RepoRoot "cdk-outputs.json"

Exec -FilePath "npx" -Args @(
  "cdk","deploy",$StackName,
  "--require-approval","never",
  "--outputs-file",$OutputsFile,
  "--parameters","EmailForAlerts=$EmailForAlerts",
  "--parameters","AlertsMode=$AlertsMode"
) -WorkingDir $InfraDir

if (!(Test-Path $OutputsFile)) { throw "CDK outputs file not found: $OutputsFile" }

# --- Read outputs ---
Write-Host "`n==> Reading CloudFormation outputs"
$json = Get-Content $OutputsFile -Raw | ConvertFrom-Json
if (-not $json.$StackName) { throw "Stack outputs not found for stack name: $StackName in $OutputsFile" }

$ArtifactsBucket = $json.$StackName.ArtifactsBucketName
$RoleArn = $json.$StackName.SageMakerExecutionRoleArn
$AlertsTopicArn = $json.$StackName.AlertsTopicArn

if (-not $ArtifactsBucket -or -not $RoleArn) {
  throw "Missing outputs. Expected: ArtifactsBucketName and SageMakerExecutionRoleArn"
}

Write-Host "ArtifactsBucketName: $ArtifactsBucket"
Write-Host "SageMakerExecutionRoleArn: $RoleArn"
Write-Host "AlertsTopicArn: $AlertsTopicArn"

# --- Upload sample data ---
Write-Host "`n==> Uploading sample data"
$LocalData = Join-Path $RepoRoot "ml\sample_data.csv"
if (!(Test-Path $LocalData)) { throw "Sample data not found: $LocalData" }

Exec -FilePath "aws" -Args @("s3","cp",$LocalData,"s3://$ArtifactsBucket/data/raw/sample_data.csv","--region",$Region)

# --- Build + run pipeline ---
Write-Host "`n==> Building + running SageMaker pipeline"
$env:AWS_REGION = $Region
$env:AWS_DEFAULT_REGION = $Region
$env:ARTIFACT_BUCKET = $ArtifactsBucket
$env:SAGEMAKER_ROLE_ARN = $RoleArn

$BuildPy = Join-Path $RepoRoot "src\pipelines\build_pipeline.py"
$RunPy   = Join-Path $RepoRoot "src\pipelines\run_pipeline.py"

if (!(Test-Path $BuildPy)) { throw "Missing file: $BuildPy" }
if (!(Test-Path $RunPy)) { throw "Missing file: $RunPy" }

Exec -FilePath $Py -Args @("-u",$BuildPy)
Exec -FilePath $Py -Args @("-u",$RunPy)

Write-Host "`n==> Done."
Write-Host "Alerts email: $EmailForAlerts"
Write-Host "AlertsMode: $AlertsMode"
Write-Host "SNS Topic: $AlertsTopicArn"
Write-Host "Monitor:"
Write-Host "  aws sagemaker list-pipeline-executions --pipeline-name $PipelineName --region $Region"