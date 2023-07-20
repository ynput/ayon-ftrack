# Receive first positional argument
Param([Parameter(Position=0)]$FunctionName)
$image = "ynput/ayon-ftrack-processor:0.1.0"
$current_dir = Get-Location
$script_dir_rel = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$script_dir = (Get-Item $script_dir_rel).FullName

function defaultfunc {
  Write-Host ""
  Write-Host "*************************"
  Write-Host "AYON ftrack processor service"
  Write-Host "   Run event processing service"
  Write-Host "*************************"
  Write-Host ""
  Write-Host "Run event processor as a service."
  Write-Host ""
  Write-Host "Usage: manage [target]"
  Write-Host ""
  Write-Host "Runtime targets:"
  Write-Host "  build    Build docker image"
  Write-Host "  clean    Remove docker image"
  Write-Host "  dev      Run docker (for development purposes)"
}

function build {
  & cp -r "$current_dir/../../ftrack_common" .
  & docker build -t "$image" .
  & Remove-Item -Recurse -Force "$current_dir/ftrack_common"
}

function clean {
  & docker rmi $(image)
}

function dev {
  & cp -r "$current_dir/../../ftrack_common" .
  & docker run --rm -ti `
    -v "$($current_dir):/service" `
  	--hostname ftrackproc `
  	--env AYON_API_KEY="verysecureapikey" `
  	--env AYON_SERVER_URL="http://localhost:5000" `
  	--env AYON_ADDON_NAME=ftrack `
  	--env AYON_ADDON_VERSION=0.1.0 `
  	"$($image)" python -m processor
  & Remove-Item -Recurse -Force "$current_dir/ftrack_common"
}

function main {
  if ($FunctionName -eq "build") {
    build
  } elseif ($FunctionName -eq "clean") {
    clean
  } elseif ($FunctionName -eq "dev") {
    dev
  } elseif ($FunctionName -eq $null) {
    defaultfunc
  } else {
    Write-Host "Unknown function ""$FunctionName"""
  }
}

main