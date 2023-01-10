# Receive first positional argument
Param([Parameter(Position=0)]$FunctionName)

$current_dir = Get-Location
$script_dir_rel = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$script_dir = (Get-Item $script_dir_rel).FullName

function install {
  & pip install -r requirements.txt
}

function run_leecher {
  & python "$($script_dir)\leecher_main.py"
}

function run_processor {
  & python "$($script_dir)\processor_main.py"
}

function main {
  $env:AY_ADDON_NAME = "ftrack"
  $env:AY_ADDON_VERSION = "0.0.1"
  $env:AY_SERVER_URL = "http://localhost:5000"
  $env:AY_API_KEY = "veryinsecurapikey"

  & "$($script_dir)\venv\Scripts\activate.ps1"
  if ($FunctionName -eq "install") {
    install
  } elseif ($FunctionName -eq "leecher") {
    run_leecher
  } elseif ($FunctionName -eq "processor") {
    run_processor
  } else {
    Write-Host "Unknown function ""$FunctionName"""
  }
  & deactivate
}

main