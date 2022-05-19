IMAGE=redun_example


setup:
	mkdir -p redun
	cp ../../../setup.py redun/setup.py
	cp -r ../../../redun redun/
	cp -r ../../../bin redun/

build:
	docker build -t $(IMAGE) .

bash:
	docker run --rm -it $(IMAGE) bash

teardown:
	docker rmi $(IMAGE)
	rm -rf redun
