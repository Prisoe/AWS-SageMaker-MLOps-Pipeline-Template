param(
  [Parameter(Mandatory=$false)][string]$Region = "us-east-1",
  [Parameter(Mandatory=$false)][double]$Threshold = 0.25,
  [Parameter(Mandatory=$false)][string]$StackName = "MlopsBlueprintStack",
  [Parameter(Mandatory=$false)][string]$VenvDir = ".venv1"
)

$ErrorActionPreference = "Stop"

function Exec {
  param(
    [Parameter(Mandatory=$true)][string]$FilePath,
    [Parameter(Mandatory=$true)][string[]]$Args,
    [Parameter(Mandatory=$false)][string]$WorkingDir = $null
  )

  if ($null -eq $Args -or $Args.Count -eq 0) {
    throw "Refusing to run '$FilePath' with empty args."
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

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$VenvPath = Join-Path $RepoRoot $VenvDir
$Py = Join-Path $VenvPath "Scripts\python.exe"

if (!(Test-Path $Py)) {
  throw "Missing venv python at: $Py. Activate/create venv first."
}

$Bucket = aws cloudformation describe-stacks `
  --stack-name $StackName `
  --region $Region `
  --query "Stacks[0].Outputs[?OutputKey=='ArtifactsBucketName'].OutputValue" `
  --output text

$SNS = aws cloudformation describe-stacks `
  --stack-name $StackName `
  --region $Region `
  --query "Stacks[0].Outputs[?OutputKey=='AlertsTopicArn'].OutputValue" `
  --output text

Write-Host "Bucket: $Bucket"
Write-Host "SNS: $SNS"

Exec -FilePath $Py -Args @(
  ".\src\monitoring\alarms.py",
  "--region", $Region,
  "--sns-topic-arn", $SNS,
  "--threshold", "$Threshold"
) -WorkingDir $RepoRoot

Exec -FilePath $Py -Args @(
  ".\src\monitoring\model_monitor_setup.py",
  "--region", $Region,
  "--baseline-s3-uri", "s3://$Bucket/data/raw/sample_data.csv",
  "--recent-s3-prefix", "s3://$Bucket/data/raw/",
  "--sns-topic-arn", $SNS,
  "--psi-threshold", "$Threshold"
) -WorkingDir $RepoRoot

Write-Host "✅ Monitoring complete"