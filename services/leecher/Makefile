VERSION=$(shell python -c "import os;import sys;content={};f=open('$(CURDIR)/../../version.py');exec(f.read(),content);f.close();print(content['__version__'])")
IMAGE := ynput/ayon-ftrack-leecher:$(VERSION)

build:
	docker build -t $(IMAGE)  .

clean:
	docker rmi $(IMAGE)

dev:
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--env AYON_API_KEY=verysecureapikey \
		--env AYON_SERVER_URL="http://172.18.0.1:5000" \
		--env AYON_ADDON_NAME=ftrack \
		--env AYON_ADDON_VERSION=$(VERSION) \
		$(IMAGE) python -m leecher
