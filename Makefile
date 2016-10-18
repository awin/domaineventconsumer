all: build

build:
	docker build -t dockerhub.zanox.com:5000/zanox/animated-octopus .

run:
	docker run --rm -it dockerhub.zanox.com:5000/zanox/animated-octopus bash

start: stop
	docker run -d --name animated-octopus dockerhub.zanox.com:5000/zanox/animated-octopus

stop:
	@docker rm -vf animated-octopus ||:

exec:
	docker exec -it animated-octopus bash

logs:
	docker exec animated-octopus sh -c "tail -f /srv/log/animated-octopus/*"

version ?= latest
push:
	docker tag -f dockerhub.zanox.com:5000/zanox/animated-octopus dockerhub.zanox.com:5000/zanox/animated-octopus:$(version)
	docker push dockerhub.zanox.com:5000/zanox/animated-octopus:$(version)

.PHONY: all build run start stop exec logs push
