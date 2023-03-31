# Receive first positional argument
Param([Parameter(Position=0)]$FunctionName)
$image = "ynput/ayon-ftrack-leecher:0.0.1"
$current_dir = Get-Location
$script_dir_rel = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
$script_dir = (Get-Item $script_dir_rel).FullName

function defaultfunc {
  Write-Host ""
  Write-Host "*************************"
  Write-Host "AYON ftrack leecher service"
  Write-Host "   Run event leecher service"
  Write-Host "*************************"
  Write-Host ""
  Write-Host "Usage: manage [target]"
  Write-Host ""
  Write-Host "Runtime targets:"
  Write-Host "  build    Build docker image"
  Write-Host "  clean    Remove docker image"
  Write-Host "  dev      Run docker (for development purposes)"
}

function build {
  & docker build -t "$image" .
}

function clean {
  & docker rmi $(image)
}

function dev {
  & docker run --rm -ti `
    -v "$($current_dir):/service" `
  	--hostname ftrackproc `
  	--env AY_API_KEY="verysecureapikey" `
  	--env AY_SERVER_URL="http://localhost:5000" `
  	--env AY_ADDON_NAME=ftrack `
  	--env AY_ADDON_VERSION=0.0.1 `
  	"$($image)" python -m leecher
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