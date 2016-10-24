all: build

build:
	docker build -t dockerhub.zanox.com:5000/animated-octopus .

run:
	docker run --rm -it dockerhub.zanox.com:5000/animated-octopus bash

.PHONY: all build run
