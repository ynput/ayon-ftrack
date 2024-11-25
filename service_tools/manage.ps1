# Receive first positional argument
$FunctionName=$ARGS[0]
$arguments=@()
if ($ARGS.Length -gt 1) {
    $arguments = $ARGS[1..($ARGS.Length - 1)]
}

$script_dir_rel = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$script_dir = (Get-Item $script_dir_rel).FullName

$ADDON_VERSION = Invoke-Expression -Command "python -c ""import os;import sys;content={};f=open(r'$($script_dir)/../package.py');exec(f.read(),content);f.close();print(content['version'])"""

function Default-Func {
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
  Write-Host "Optional arguments for service targets:"
  Write-Host "--variant [variant] (Define settings variant. default: 'production')"
  Write-Host ""
  Write-Host "Runtime targets:"
  Write-Host "  install      Install requirements to currently actie python (recommended to create venv)"
  Write-Host "  leecher      Start leecher of ftrack events"
  Write-Host "  processor    Main processing logic"
  Write-Host "  transmitter  AYON to ftrack sync"
  Write-Host "  ftrack2ayon  Services related to ftrack to AYON sync"
  Write-Host "  ayon2ftrack  Services related to AYON to ftrack sync"
  Write-Host "  services     Start both leecher and processor (experimental)"
  Write-Host ""
}

function Install-Requirements {
  # TODO Install/verify venv is created
  & python -m pip install -r "$($script_dir)\requirements.txt"
}

function Start-Leecher {
  & python "$($script_dir)\main.py" --service leecher @arguments
}

function Start-Processor {
  & python "$($script_dir)\main.py" --service processor @arguments
}

function Start-AYONToftrack {
  & python "$($script_dir)\main.py" --service transmitter @arguments
}

function Start-Leecher-Processor {
  & python "$($script_dir)\main.py" --service ftrack2ayon @arguments
}

function Start-All {
  & python "$($script_dir)\main.py" --service all @arguments
}

function Load-Env {
  $env_path = "$($script_dir)/.env"
  if (Test-Path $env_path) {
    Get-Content $env_path | foreach {
      $name, $value = $_.split("=")
      if (-not([string]::IsNullOrWhiteSpace($name) -or $name.Contains("#"))) {
        Set-Content env:\$name $value
      }
    }
  }
}

function Activate-Venv {
  # Make sure venv is created
  $venv_path = "$($script_dir)\venv"
  if (-not(Test-Path $venv_path)) {
    & python -m venv $venv_path
  }
  & "$($venv_path)\Scripts\activate.ps1"
}

function main {
  if ($null -eq $FunctionName) {
    Default-Func
    return
  }
  $env:AYON_ADDON_NAME = "ftrack"
  $env:AYON_ADDON_VERSION = $ADDON_VERSION
  Load-Env
  Activate-Venv

  try {
    if ($FunctionName -eq "install") {
      Install-Requirements
    } elseif ($FunctionName -eq "leecher") {
      Start-Leecher
    } elseif ($FunctionName -eq "processor") {
      Start-Processor
    } elseif (($FunctionName -eq "transmitter") -or ($FunctionName -eq "ayon2ftrack")) {
      Start-AYONToftrack
    } elseif ($FunctionName -eq "ftrack2ayon") {
      Start-Leecher-Processor
    } elseif ($FunctionName -eq "services") {
      Start-All
    } else {
      Write-Host "Unknown function ""$FunctionName"""
      Default-Func
    }
  } finally {
    & deactivate
  }
}

main