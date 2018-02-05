ORGANIZATION=hyperpilot
IMAGE=prometheus-adapter
TAG=latest

.PHONY: build docker-build docker-push

build:
	rm -rf bin/*
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/linux/$(IMAGE) ./cmd
docker-build:
	docker build -t $(ORGANIZATION)/$(IMAGE):$(TAG) .
docker-push: build docker-build
	docker push $(ORGANIZATION)/$(IMAGE):$(TAG)
