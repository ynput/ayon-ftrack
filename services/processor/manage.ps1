# Receive first positional argument
Param([Parameter(Position=0)]$FunctionName)
$current_dir = Get-Location
$script_dir_rel = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$script_dir = (Get-Item $script_dir_rel).FullName

$IMAGE_NAME = "ynput/ayon-ftrack-processor"
$ADDON_VERSION = Invoke-Expression -Command "python -c ""import os;import sys;content={};f=open(r'$($script_dir)/../../version.py');exec(f.read(),content);f.close();print(content['__version__'])"""
$IMAGE_FULL_NAME = "$($IMAGE_NAME):$($ADDON_VERSION)"

function defaultfunc {
  Write-Host ""
  Write-Host "*************************"
  Write-Host "AYON ftrack processor service"
  Write-Host "   Run event processing service"
  Write-Host "   Docker image name: $($IMAGE_FULL_NAME)"
  Write-Host "*************************"
  Write-Host ""
  Write-Host "Run event processor as a service."
  Write-Host ""
  Write-Host "Usage: manage [target]"
  Write-Host ""
  Write-Host "Runtime targets:"
  Write-Host "  build    Build docker image"
  Write-Host "  clean    Remove docker image"
  Write-Host "  dist     Publish docker image to docker hub"
  Write-Host "  dev      Run docker (for development purposes)"
}

function build {
  & cp -r "$current_dir/../../ftrack_common" .
  & docker build -t "$IMAGE_FULL_NAME" .
  & Remove-Item -Recurse -Force "$current_dir/ftrack_common"
}

function clean {
  & docker rmi $(IMAGE_FULL_NAME)
}

function dist {
  build
  # Publish the docker image to the registry
  docker push "$IMAGE_FULL_NAME"
}

function dev {
  & cp -r "$current_dir/../../ftrack_common" .
  & docker run --rm -ti `
    -v "$($current_dir):/service" `
  	--hostname ftrackproc `
  	--env AYON_API_KEY="verysecureapikey" `
  	--env AYON_SERVER_URL="http://localhost:5000" `
  	--env AYON_ADDON_NAME=ftrack `
  	--env AYON_ADDON_VERSION=$ADDON_VERSION `
  	"$IMAGE_FULL_NAME" python -m processor
  & Remove-Item -Recurse -Force "$current_dir/ftrack_common"
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
  } elseif ($FunctionName -eq $null) {
    defaultfunc
  } else {
    Write-Host "Unknown function ""$FunctionName"""
  }
}

main