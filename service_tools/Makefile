ifneq (,$(wildcard ./.env))
		include .env
		export
endif

VERSION=$(shell python -c "import os;import sys;content={};f=open('$(CURDIR)/../package.py');exec(f.read(),content);f.close();print(content['version'])")

define HELP_INFO

*************************
AYON ftrack services tool
	 Run ftrack services
*************************

Run service processes from terminal. It is recommended to use docker images for production.

Usage: make [target]

Optional arguments for service targets:
--variant [variant] (Define settings variant. default: 'production')

Runtime targets:
	install    Install requirements to currently actie python (recommended to create venv)
	leecher    Start leecher of ftrack events
	processor  Main processing logic
	services   Start both leecher and processor (experimental)

endef
export HELP_INFO

help:
	@echo "$$HELP_INFO"

install:
		$(shell [[ -d $(CURDIR)/venv ]] || python -m venv $(CURDIR)/venv) && \
		. $(CURDIR)/venv/bin/activate && python -m pip install -r "$(CURDIR)/requirements.txt"

leecher:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/venv/bin/activate && \
		python "$(CURDIR)/main.py" --service leecher $@

processor:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/venv/bin/activate && \
		python "$(CURDIR)/main.py" --service processor $@

transmitter:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/venv/bin/activate && \
		python "$(CURDIR)/main.py" --service transmitter $@

ayon2ftrack: transmitter

ftrack2ayon:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/venv/bin/activate && \
		python "$(CURDIR)/main.py" --service ftrack2ayon $@

services:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/venv/bin/activate && \
		python "$(CURDIR)/main.py" --service all $@

info: help
default: help
