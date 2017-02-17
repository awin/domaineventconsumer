all: build

build:
	docker build -t registry.zanox.com/animated-octopus .

run:
	docker run --rm -it registry.zanox.com/animated-octopus bash

.PHONY: all build run
