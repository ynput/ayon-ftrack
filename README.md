# Ftrack integration for AYON

This project provides three elements for the AYON pipeline:
 * server - The AYON backend Addon.
 * client - The AYON (currently OpenPype) desktop integration.
 * services - Standalone dockerized daemons that act based on events (aka `leecher` and `processors`).

There is a common code that can be re-used for `server`, `client` and `services`. Is located inside client code for developer mode `./client/ayon_ftrack/common/`.

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

## Create package
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

### Start with prepared tools (Development & Testing)
Tools require to have available Python 3.9. Prepared scripts can be found in `./service_tools` directory. There are 2 scripts `start.ps1` for Windows and `Makefile` for Linux. Both scripts are doing the same thing and have same commands.

Make sure you run `make install` (linux) or `./start.ps1 install` before running any other command.

For more information check [README](service_tools/README.md).

## Development & Testing
Development and testing of this addon is complicated.

### Server
Server code must be uploaded (like with all other addons). Run `python create_package.py` to create package that can be uploaded to the server.

### Client
Point dev path to `./client/` folder inside the repository.

### Services
Services can be tested in 2 ways. One is by running them locally using prepared service tools (see above).

Second is by running them as docker containers. For that you need to have running ASH (AYON service host). Once ASH is running you can run services from AYON web UI. This is recommended approach how to run services in production. But for testing of Pull requests it is required to build the docker image manually instead of using images from docker hub.
Images must be built for all services.

#### Windows
```shell
cd ./services/leecher
./manage.ps1 build

cd ../processor
./manage.ps1 build
```

#### Linux
```shell
cd ./services/leecher
make build

cd ../processor
make build
```
