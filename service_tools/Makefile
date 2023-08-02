include .env
define HELP_INFO

*************************
AYON ftrack services tool
   Run ftrack services
*************************

Run service processes from terminal. It is recommended to use docker images for production.

Usage: make [target]

Runtime targets:
  install    Install requirements to currently actie python (recommended to create venv)
  leecher    Start leecher of ftrack events
  processor  Main processing logic

endef
export HELP_INFO

help:
	@echo "$$HELP_INFO"

install:
	$(CURDIR)/venv/Scripts/activate
	python -m pip install -r "$(CURDIR)/requirements.txt"
	deactivate

leecher:
	$(CURDIR)/venv/Scripts/activate
	python "$(CURDIR)/leecher_main.py"
	deactivate

processor:
	$(CURDIR)/venv/Scripts/activate
	python "$(CURDIR)/processor_main.py"
	deactivate

info: help
default: help
