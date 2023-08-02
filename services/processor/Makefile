include .env
VERSION=$(shell python -c "import os;import sys;content={};f=open('$(CURDIR)/../../version.py');exec(f.read(),content);f.close();print(content['__version__'])")
IMAGE := ynput/ayon-ftrack-processor:$(VERSION)

build:
	cp -r ../../ftrack_common .
	docker build -t $(IMAGE) .
	rm -rf ftrack_common

clean:
	docker rmi $(image)

dev:
	cp -r ../../ftrack_common .
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--hostname ftrackproc \
		--env AYON_API_KEY=${AYON_API_KEY} \
		--env AYON_SERVER_URL=${AYON_SERVER_URL} \
		--env AYON_ADDON_NAME=ftrack \
		--env AYON_ADDON_VERSION=$(VERSION) \
		$(IMAGE) python -m processor
	rm -rf ftrack_common
