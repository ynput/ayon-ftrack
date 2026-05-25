ifneq (,$(wildcard ./.env))
		include .env
		export
endif

VERSION=$(shell python -c "import os;import sys;content={};f=open('$(CURDIR)/../package.py');exec(f.read(),content);f.close();print(content['version'])")
ARGS ?=

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
	install    Install requirements
	leecher    Start leecher of ftrack events
	processor  Main processing logic
	services   Start both leecher and processor (experimental)

endef
export HELP_INFO

help:
	@echo "$$HELP_INFO"

install:
		uv sync

leecher:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/.venv/bin/activate && \
		uv run "$(CURDIR)/main.py" --service leecher $(ARGS)

processor:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/.venv/bin/activate && \
		uv run "$(CURDIR)/main.py" --service processor $(ARGS)

transmitter:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/.venv/bin/activate && \
		uv run "$(CURDIR)/main.py" --service transmitter $(ARGS)

ayon2ftrack: transmitter

ftrack2ayon:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/.venv/bin/activate && \
		python "$(CURDIR)/main.py" --service ftrack2ayon $(ARGS)

services:
		export AYON_ADDON_NAME="ftrack" && \
		export AYON_ADDON_VERSION=$(VERSION) && \
		. $(CURDIR)/.venv/bin/activate && \
		python "$(CURDIR)/main.py" --service all $(ARGS)

info: help
default: help
