## Service tools
Helper tools to develop ftrack services or parts of the services.

### How to run
At this moment is available only PowerShell script. All commands expect that there is created virtual environment in `./venv`. Script also contains hardcoded url to server and API key.

### Commands
- `install` - install requirements needed for running processed (requires Git)
- `leecher` - start leecher
- `processor` - start processor
### Leecher 
Ftrack leecher postpone ftrack events into Ayon event database. Is separated from processor to be able restart or have different ftrack processors for different purposes loading events from single place. Using Ayon server as middle-ware helps to know which event was already processed or is processing. In theory one event should not be processed multiple times. 

### Processor
Processor of ftrack events. Is not loading events from ftrack but from Ayon database. Can get only one ftrack event at once and if there is other running processor processing events under same identifier it won't continue to process next events until that is finished. That is due to race condition issues that may happen. Processor requires to have running **leecher**.

### Todos
- Add linux/macos scripts.
- Get rid of hardcoded environments in script.
- Replace `ayclient` with different helper - `ayclient` was created in rush to be have access to server url from docker service.
- Processor should be split into multiple services. Sync from ftrack logic requires to process all events in very deterministic order but in some cases the event order does not affect the result so much.
