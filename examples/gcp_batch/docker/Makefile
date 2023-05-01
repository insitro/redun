IMAGE?=redun_example
REGION?=us-west1
PROJECT?=$(shell gcloud config get-value project)
REGISTRY?=$(REGION)-docker.pkg.dev/$(PROJECT)/redun-example


setup:
	mkdir -p redun
	cp ../../../setup.py redun/setup.py
	cp -r ../../../redun redun/
	cp -r ../../../bin redun/


login:
	gcloud auth configure-docker $(REGION)-docker.pkg.dev

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
