image := ynput/ayon-ftrack-leecher:0.1.0

build:
	docker build -t $(image)  . 

clean:
	docker rmi $(image)

dev:
	docker run --rm -ti \
		-v $(CURDIR):/service \
		--env AYON_API_KEY=verysecureapikey \
		--env AYON_SERVER_URL="http://172.18.0.1:5000" \
		--env AYON_ADDON_NAME=ftrack \
		--env AYON_ADDON_VERSION=0.1.0 \
		$(image) python -m leecher
