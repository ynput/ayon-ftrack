image := ynput/ayon-ftrack-leecher:0.0.1

build:
	docker build -t $(image)  . 

clean:
	docker rmi $(image)

dev:
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--env AY_API_KEY=verysecureapikey \
		--env AY_SERVER_URL="http://172.18.0.1:5000" \
		--env AY_ADDON_NAME=ftrack \
		--env AY_ADDON_VERSION=1.0.0 \
		$(image) python -m leecher
