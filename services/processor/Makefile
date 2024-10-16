ENVFILE = .env
ENVFILE_EXAMPLE = example_env

# Create the .env file if not exit.
ifeq ("$(wildcard $(ENVFILE))","")
   $(shell cp $(ENVFILE_EXAMPLE) $(ENVFILE))
endif

VERSION=$(shell python -c "import os;import sys;content={};f=open('$(CURDIR)/../../package.py');exec(f.read(),content);f.close();print(content['version'])")
BASE_NAME := ayon-ftrack-processor
IMAGE := ynput/$(BASE_NAME):$(VERSION)
BASH_CONTAINER_NAME := $(BASE_NAME)-bash-$(VERSION)

build:
	cp -r ../../client/ayon_ftrack/common/ ./ftrack_common
	docker build -t $(IMAGE) .
	rm -rf ftrack_common

dist:
	docker push $(IMAGE)

clean:
	docker rmi $(image)

dev:
	cp -r ../../client/ayon_ftrack/common/ ./ftrack_common
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--hostname ftrackproc \
		--env AYON_API_KEY=${AYON_API_KEY} \
		--env AYON_SERVER_URL=${AYON_SERVER_URL} \
		--env AYON_ADDON_NAME=ftrack \
		--env AYON_ADDON_VERSION=$(VERSION) \
		$(IMAGE) python -m processor
	rm -rf ftrack_common

bash:
	docker run --name $(BASH_CONTAINER_NAME) --rm -it --entrypoint /bin/bash $(IMAGE)
