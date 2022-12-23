image := ynput/ayon-ftrack-processor:0.0.1

build:
	cp -r ../../ftrack_common .
	docker build -t $(image)  . 
	rm -rf ftrack_common

clean:
	docker rmi $(image)

dev:
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--hostname ftrackproc \
		--env AY_API_KEY=verysecureapikey \
		--env AY_SERVER_URL="http://172.18.0.1:5000" \
		--env AY_ADDON_NAME=ftrack \
		--env AY_ADDON_VERSION=1.0.0 \
		$(image) python -m processor
