IMAGE=redun_example_bioinfo_batch
ACCOUNT=$(shell aws ecr describe-registry --query registryId --output text)
REGION=$(shell aws configure get region)
REGISTRY=$(ACCOUNT).dkr.ecr.$(REGION).amazonaws.com


login:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(REGISTRY)

build:
	docker build -t $(REGISTRY)/$(IMAGE) .

build-local:
	docker build -t $(IMAGE) .

create-repo:
	aws ecr create-repository --repository=$(IMAGE)

push:
	docker push $(REGISTRY)/$(IMAGE)

bash:
	docker run --rm -it $(REGISTRY)/$(IMAGE) bash

bash-local:
	docker run --rm -it $(IMAGE) bash
