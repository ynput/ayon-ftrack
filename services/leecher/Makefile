ENVFILE = .env
ENVFILE_EXAMPLE = example_env

# Create the .env file if not exit.
ifeq ("$(wildcard $(ENVFILE))","")
   $(shell cp $(ENVFILE_EXAMPLE) $(ENVFILE))
endif

VERSION=$(shell python -c "import os;import sys;content={};f=open('$(CURDIR)/../../package.py');exec(f.read(),content);f.close();print(content['version'])")
BASE_NAME := ayon-ftrack-leecher
IMAGE := ynput/$(BASE_NAME):$(VERSION)
BASH_CONTAINER_NAME := $(BASE_NAME)-bash-$(VERSION)

build:
	docker build -t $(IMAGE)  .

dist:
	docker push $(IMAGE)

clean:
	docker rmi $(IMAGE)

dev:
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--env AYON_API_KEY=${AYON_API_KEY} \
		--env AYON_SERVER_URL=${AYON_SERVER_URL} \
		--env AYON_ADDON_NAME=ftrack \
		--env AYON_ADDON_VERSION=$(VERSION) \
		$(IMAGE) python -m leecher

bash:
	docker run --name $(BASH_CONTAINER_NAME) --rm -it --entrypoint /bin/bash $(IMAGE)
