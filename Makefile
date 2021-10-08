PACKAGE_NAME=redun
VENV_DIR?=.venv
VENV_ACTIVATE=$(VENV_DIR)/bin/activate
WITH_VENV=. $(VENV_ACTIVATE);


.PHONY: venv
venv: $(VENV_ACTIVATE)


$(VENV_ACTIVATE):
	test -f $@ || python3 -m venv $(VENV_DIR)
	$(WITH_VENV) pip install --upgrade pip
	$(WITH_VENV) pip install -e .[postgres]
	$(WITH_VENV) pip install -r requirements-dev.txt


.PHONY: setup
setup: venv


.PHONY: test
test: venv
	$(WITH_VENV) tox
	make test-postgres


.PHONY: test-postgres
test-postgres: venv
	bin/test_postgres.sh


.PHONY: lint
lint: lint-python mypy black isort


.PHONY: black
black: venv
	$(WITH_VENV) black --check $(PACKAGE_NAME)

.PHONY: isort
isort: venv
	$(WITH_VENV) isort --check-only $(PACKAGE_NAME)

.PHONY: format
format: venv
	$(WITH_VENV) isort $(PACKAGE_NAME)
	$(WITH_VENV) black $(PACKAGE_NAME)


.PHONY: lint-python
lint-python: venv
	$(WITH_VENV) flake8 $(PACKAGE_NAME)


.PHONY: mypy
mypy: venv
	$(WITH_VENV) mypy $(PACKAGE_NAME)


# Migrate example database to latest version.
.PHONY: upgrade
upgrade:
	$(WITH_VENV) alembic -c redun/backends/db/alembic.ini upgrade head


# Autogenerate a new migration
.PHONY: revision
revision:
	$(WITH_VENV) alembic -c redun/backends/db/alembic.ini revision --autogenerate -m "$(MESSAGE)"


.PHONY: build
build: setup
	.venv/bin/python setup.py sdist


.PHONY: test-build
test-build:
	python3 -m venv build/venv
	build/venv/bin/pip install dist/redun-$(shell python3 setup.py --version).tar.gz
	bin/test_build.sh


.PHONY: publish
publish: lint test build
	bin/publish-packages.sh


.PHONY: docs
docs: venv
	$(WITH_VENV) cd docs; make clean api html


.PHONY: docs
pub-docs:
	bin/publish-docs.sh


.PHONY: clean
clean:
	rm -rf build
	rm -rf dist
	rm -rf *.egg*/
	find $(PACKAGE_NAME) -type f -name '*.pyc' -delete


.PHONY: teardown
teardown:
	rm -rf $(VENV_DIR)/
	rm -rf .tox/
