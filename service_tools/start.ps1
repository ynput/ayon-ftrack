# Receive first positional argument
Param([Parameter(Position=0)]$FunctionName)

$current_dir = Get-Location
$script_dir_rel = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$script_dir = (Get-Item $script_dir_rel).FullName

function defaultfunc {
  Write-Host ""
  Write-Host "*************************"
  Write-Host "AYON ftrack services tool"
  Write-Host "   Run ftrack services"
  Write-Host "*************************"
  Write-Host ""
  Write-Host "Run service processes from terminal. It is recommended to use docker images for production."
  Write-Host ""
  Write-Host "Usage: start [target]"
  Write-Host ""
  Write-Host "Runtime targets:"
  Write-Host "  install    Install requirements to currently actie python (recommended to create venv)"
  Write-Host "  leecher    Start leecher of ftrack events"
  Write-Host "  processor  Main processing logic"
}

function install {
  # TODO Install/verify venv is created
  & python -m pip install -r "$($script_dir)\requirements.txt"
}

function run_leecher {
  & python "$($script_dir)\leecher_main.py"
}

function run_processor {
  & python "$($script_dir)\processor_main.py"
}

function main {
  $env:AYON_ADDON_NAME = "ftrack"
  $env:AYON_ADDON_VERSION = "0.1.0"
  $env:AYON_SERVER_URL = "http://localhost:5000"
  $env:AYON_API_KEY = "verysecureapikey"

  & "$($script_dir)\venv\Scripts\activate.ps1"
  if ($FunctionName -eq "install") {
    install
  } elseif ($FunctionName -eq "leecher") {
    run_leecher
  } elseif ($FunctionName -eq "processor") {
    run_processor
  } elseif ($FunctionName -eq $null) {
    defaultfunc
  } else {
    Write-Host "Unknown function ""$FunctionName"""
  }
  & deactivate
}

main