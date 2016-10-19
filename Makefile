all: build

build:
	docker build -t dockerhub.zanox.com:5000/animated-octopus .

run:
	docker run --rm -it dockerhub.zanox.com:5000/animated-octopus bash

start: stop
	docker run -d --name animated-octopus dockerhub.zanox.com:5000/animated-octopus

stop:
	@docker rm -vf animated-octopus ||:

exec:
	docker exec -it animated-octopus bash

logs:
	docker exec animated-octopus sh -c "tail -f /srv/log/animated-octopus/*"

version ?= latest
push:
	docker tag -f dockerhub.zanox.com:5000/animated-octopus dockerhub.zanox.com:5000/animated-octopus:$(version)
	docker push dockerhub.zanox.com:5000/animated-octopus:$(version)

redis-commander:
	docker run -d -p 80:8081 tenstartups/redis-commander --redis-host d-lhr1-docker-141.zanox.com

.PHONY: all build run start stop exec logs push redis-commander
