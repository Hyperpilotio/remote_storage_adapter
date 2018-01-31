ORGANIZATION=jpra1113
IMAGE=prometheus_adapter
TAG=latest

.PHONY: build docker-build docker-push

build:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo
docker-build:
	sudo docker build -t $(ORGANIZATION)/$(IMAGE):$(TAG) .
docker-push: build docker-build
	sudo docker push $(ORGANIZATION)/$(IMAGE):$(TAG)
