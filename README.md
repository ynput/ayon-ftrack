# Ftrack integration for AYON

This project provides three elements for the AYON pipeline:
 * server - The AYON backend Addon.
 * client - The AYON (currently OpenPype) desktop integration.
 * services - Standalone dockerized daemons that act based on events (aka `leecher` and `processors`).

The `ftrack_common` directory contains re-usable code for `server`, `client` and `services`.

## Server
Once loaded into the backend, restart your server to update addons, ftrack addon will care about creation of attributes for entities. Addon must be enabled in Addon versions the plugin itself can be configured from the Project Settings page: `{ayon_url}/projectManager/projectSettings`, where you can specify your Ftrack instance URL.

### Settings
Settings for services and client.

### Public
Contains publicly available content, like icons. Icons are used by ftrack server to display action icons. A public is used as it does not require authentication.

### Private
Primarily for client code downloadable file.

## Client
Contains ftrack integration used in ayon launcher application. Contains publish plugins with logic to integrate content to ftrack like reviewables. Also contains ftrack server for user with actions that are executed inside ayon launcher application, like delivery creation, applications launch etc.

## Services
Currently, there is `leecher` which stores ftrack events and `processor` which is processing them. Separation of these two tasks allow to restart `processor` without loosing any events that happened meanwhile. There can be multiple services running at the same time. The `processer` has nothing to process without runnin `leecher`.

## Create pakcage
To create a "server-ready" package of the `server` folder, on a terminal, run `python create_package.py`. That will create `./package/ftrack {version}.zip` file that can be uploaded to the server.

## Services
As mentioned there are 2 services `leecher` and `processor`. Both services have docker images that can be started from AYON server. For that there must be running a docker worker called ASH (AYON service host). Once ASH is running you can run services from AYON web UI. This is recommended approach how to run services in production.

To run services locally (recommended only for development purposes), there are 2 possible approaches. One is by running docker image, or using prepared service tools.

### Start as docker
Both services have prepared scripts to build and run docker images. There are 2 scripts `manage.ps1` for Windows and `Makefile` for Linux. Both scripts are doing the same thing and have same commands.

#### Commands:
- `build` - Build docker image
- `dev` - Run docker image in development mode (uses .env file to define AYON server credentials)
- `dist` - Push docker image to docker hub (don't use this command please)

#### Docker compose
There are also `docker-compose.yml` files that will create a docker stack if you run `docker-compose up -d`. But run `build` command first so the image is available. 

### Start with prepared tools
Tools require to have available Python 3.9. Prepared scripts can be found in `./service_tools` directory. There are 2 scripts `start.ps1` for Windows and `Makefile` for Linux. Both scripts are doing the same thing and have same commands.

Commands:
- `install` - Create venv and install dependencies (run only once) 
- `leecher` - Start leecher process
- `processor` - Start processor process

These scripts depend on the existance of a `.env` file which you need to put here: `./service_tools/.env`. The contents of the file should be:

1. on linux:

```
export AYON_API_KEY={insert your ftrack api key}
export AYON_SERVER_URL={your ayon server url:5000}
export AYON_ADDON_NAME=ftrack
export AYON_ADDON_VERSION={the addon version}
```

2. on windows:

```
AYON_API_KEY={insert your ftrack api key}
AYON_SERVER_URL={your ayon server url:5000}
AYON_ADDON_NAME=ftrack
AYON_ADDON_VERSION={the addon version}
```

where you replace the stuff in the curly braces with your details.