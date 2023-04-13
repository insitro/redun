IMAGE=redun_example
REGION=us-west1
PROJECT=$(shell gcloud config get-value project)
REGISTRY=$(REGION)-docker.pkg.dev/$(PROJECT)/redun-example
#ACCOUNT=$(shell aws ecr describe-registry --query registryId --output text)
#REGION=$(shell aws configure get region)
#REGISTRY=$(ACCOUNT).dkr.ecr.$(REGION).amazonaws.com


setup:
	mkdir -p redun
	cp ../../../setup.py redun/setup.py
	cp -r ../../../redun redun/
	cp -r ../../../bin redun/


login:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(REGISTRY)

build:
	docker build -t $(REGISTRY)/$(IMAGE) .

build-local:
	docker build -t $(IMAGE) .

create-repo:
	gcloud artifacts repositories create --repository-format=docker --location=$(REGION) redun-example

push:
	docker push $(REGISTRY)/$(IMAGE)

bash:
	docker run --rm -it $(REGISTRY)/$(IMAGE) bash

bash-local:
	docker run --rm -it $(IMAGE) bash

teardown:
	rm -rf redun
