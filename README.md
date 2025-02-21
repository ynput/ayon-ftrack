# ftrack integration for AYON

This project provides three elements for the AYON pipeline:
 * server - The AYON backend Addon.
 * client - The AYON desktop integration.
 * services - Standalone dockerized daemons that act based on events (aka `leecher` and `processors`).

There is a common code that can be re-used for `server`, `client` and `services`. Is located inside client code for developer mode `./client/ayon_ftrack/common/`.

## Server
Once loaded into the backend, restart your server to update addons, ftrack addon will care about creation of attributes for entities. Addon must be enabled in Addon versions the plugin itself can be configured from the Project Settings page: `{ayon_url}/projectManager/projectSettings`, where you can specify your ftrack instance URL.

### Settings
Settings for services and client.

### Public
Contains publicly available content, like icons. Icons are used by ftrack server to display action icons. A public is used as it does not require authentication.

### Private
Primarily for client code downloadable file.

## Client
Contains ftrack integration used in ayon launcher application. Contains publish plugins with logic to integrate content to ftrack like reviewables. Also contains ftrack server for user with actions that are executed inside ayon launcher application, like delivery creation, applications launch etc.

## Services
Currently, there is `leecher` which stores ftrack events, `processor` which is processing them and `transmitter` which propagates changes from AYON to ftrack. Separation of `leecher` and `processor` allows to restart `processor` without loosing any events that happened meanwhile. The `processer` has nothing to process without running `leecher`. There can be multiple services running at the same time, but they all should be using same version and settings variant.

## Create package
To create a "server-ready" package of the `server` folder, on a terminal, run `python create_package.py`. That will create `./package/ftrack {version}.zip` file that can be uploaded to the server.

## Services
As mentioned there are 2 services `leecher` and `processor`. Both services have docker images that can be started from AYON server. For that there must be running a docker worker called ASH (AYON service host). Once ASH is running you can run services from AYON web UI. This is recommended approach how to run services in production.

To run services locally (recommended only for development purposes), there are 2 possible approaches. One is by running docker image, or using prepared service tools.

- `leecher` - Service that listens to ftrack events and stores them in the AYON database.
- `processor` - Service that is processing ftrack events stored in the AYON database. Only one event is processed at a time.

### Processor
Processor contains multiple event handlers that handle synchronization or basic automations helpers. It also provides a way to add custom event handlers from other addons. The addon must be loaded into the server, and must be available in bundle based on variant that service is running in ("production", "staging" or dev bundle).
The addon also must have prepared archive file that can be downloaded from the server.

#### Archive file
The archive file can be a zip or tar, must contain `manifest.json` file that describes the content. The archive file must be uploaded to the server and must be available for download. The addon must implement `get_custom_ftrack_handlers_endpoint` method that returns URL to the archive file.

```python
class SomeAddon(BaseServerAddon):
    name = "some_addon"
    version = "1.0.0"
    
    def get_custom_ftrack_handlers_endpoint(self) -> str:
        return "addons/{self.name}/{self.version}/private/ftrack_handlers.tar.gz"
```

#### Manifest file
Manifest file is a JSON file that describes the content of the archive file. It is used to load the content of the archive file into the processor. The file must be named `manifest.json` and must be in the root of the archive file.

```json
{
    "version": "1.0.0",
    "handler_subfolders": [
        "event_handlers"
    ],
    "python_path_subfolders": [
        "common"
    ]
}
```
Content of manifect may change in future, to be able to track changes and keep backwards compatibilit a `"version"` was added. Current version is `"1.0.0"`.

<b>1.0.0</b>
- `handler_subfolders` - List of subfolder, relative to manifest.json where files with event handlers can be found. Processor will go through all of the subfolders and import all python files that are in the subfolder. It is recommended to have only one subfolder.
- `python_path_subfolders` - Optional list of subfolders, relative to manifest.json. These paths are added to `sys.path` so content inside can be imported. Can be used for "common" code for the event handlers. It is not recommended to add python modules because of possible conflicts with other addons, but is possible.


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
