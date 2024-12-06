# Receive first positional argument
$FunctionName=$ARGS[0]
$arguments=@()
if ($ARGS.Length -gt 1) {
    $arguments = $ARGS[1..($ARGS.Length - 1)]
}

$script_dir_rel = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$script_dir = (Get-Item $script_dir_rel).FullName

$BASE_NAME = "ayon-ftrack-transmitter"
$IMAGE_NAME = "ynput/$($BASE_NAME)"
$ADDON_VERSION = Invoke-Expression -Command "python -c ""import os;import sys;content={};f=open(r'$($script_dir)/../../package.py');exec(f.read(),content);f.close();print(content['version'])"""
$IMAGE_FULL_NAME = "$($IMAGE_NAME):$($ADDON_VERSION)"
$BASH_CONTAINER_NAME = "$($BASE_NAME)-bash-$($ADDON_VERSION)"

function defaultfunc {
  Write-Host ""
  Write-Host "*************************"
  Write-Host "AYON to ftrack sync service"
  Write-Host "   Run event sync service"
  Write-Host "   Docker image name: $($IMAGE_FULL_NAME)"
  Write-Host "*************************"
  Write-Host ""
  Write-Host "Usage: manage [target]"
  Write-Host ""
  Write-Host "Runtime targets:"
  Write-Host "  build    Build docker image"
  Write-Host "  clean    Remove docker image"
  Write-Host "  dist     Publish docker image to docker hub"
  Write-Host "  dev      Run docker (for development purposes)"
  Write-Host "  bash     Run bash in docker image (for development purposes)"
  Write-Host ""
}

function build {
  & docker build -t "$IMAGE_FULL_NAME" .
}

function clean {
  & docker rmi $(IMAGE_FULL_NAME)
}

function dist {
  build
  # Publish the docker image to the registry
  docker push "$IMAGE_FULL_NAME"
}

function load-env {
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

function dev {
  load-env
  & docker run --rm -ti `
    -v "$($script_dir):/service" `
  	--hostname ftrackproc `
  	--env AYON_API_KEY=$env:AYON_API_KEY `
  	--env AYON_SERVER_URL=$env:AYON_SERVER_URL `
  	--env AYON_ADDON_NAME=ftrack `
  	--env AYON_ADDON_VERSION=$ADDON_VERSION `
  	"$($IMAGE_FULL_NAME)" python -m transmitter
}

function bash {
  & docker run --name "$($BASH_CONTAINER_NAME)" --rm -it --entrypoint /bin/bash "$($IMAGE_FULL_NAME)"
}

function main {
  if ($FunctionName -eq "build") {
    build
  } elseif ($FunctionName -eq "clean") {
    clean
  } elseif ($FunctionName -eq "dev") {
    dev
  } elseif ($FunctionName -eq "dist") {
    dist
  } elseif ($FunctionName -eq "bash") {
    bash
  } elseif ($FunctionName -eq $null) {
    defaultfunc
  } else {
    Write-Host "Unknown function ""$FunctionName"""
  }
}

main