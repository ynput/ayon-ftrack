## Service tools
Helper tools to develop ftrack services or parts of the services.

### How to run
At this moment there is available PowerShell script and Makefile. All commands expect that there is created virtual environment in `./venv`. These scripts depend on the existence of a `./.env`, use `example_env` as template. The contents of the file should be:
```
AYON_SERVER_URL={AYON server url}
AYON_API_KEY={AYON server api key (ideally service user)}
```

### Commands
- `install` - install requirements needed for running processed (requires Git)
- `leecher` - start leecher
- `processor` - start processor
- `services` - start both leecher and processor services (experimental)

### Leecher 
Leecher postpone ftrack events into Ayon event database. Is separated from processor to be able restart or have different ftrack processors for different purposes loading events from single place. Using Ayon server as middle-ware helps to know which event was already processed or is processing. In theory one event should not be processed multiple times. 

### Processor
Processor of ftrack events. Is not loading events from ftrack but from Ayon database. Can get only one ftrack event at once and if there is other running processor processing events under same identifier it won't continue to process next events until that is finished. That is due to race condition issues that may happen. Processor requires to have running **leecher**.

### Todos
- Processor should be split into multiple services. Sync to AYON logic requires to process all events in very deterministic order but in some cases the event order does not affect the result so much.
