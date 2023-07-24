image := ynput/ayon-ftrack-processor:0.1.0

build:
	cp -r ../../ftrack_common .
	docker build -t $(image) .
	rm -rf ftrack_common

clean:
	docker rmi $(image)

dev:
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--hostname ftrackproc \
		--env AYON_API_KEY=verysecureapikey \
		--env AYON_SERVER_URL="http://172.18.0.1:5000" \
		--env AYON_ADDON_NAME=ftrack \
		--env AYON_ADDON_VERSION=0.1.0 \
		$(image) python -m processor
