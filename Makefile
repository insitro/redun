.PHONY: setup
setup:
	uv sync --all-extras
	uv run pre-commit install
	uv tool install tox --with tox-uv

.PHONY: test
test:
	tox
	make test-postgres


.PHONY: test-postgres
test-postgres:
	bin/test_postgres.sh


.PHONY: lint
lint:
	uv run pre-commit run --all-files

.PHONY: format
format:
	uv run pre-commit run ruff-format --all-files

# Migrate example database to latest version.
.PHONY: upgrade
upgrade:
	uv run alembic -c redun/backends/db/alembic.ini upgrade head


# Autogenerate a new migration
.PHONY: revision
revision:
	uv run alembic -c redun/backends/db/alembic.ini revision --autogenerate -m "$(MESSAGE)"


.PHONY: build
build: setup
	uv build


.PHONY: publish
publish: lint test build
	bin/publish-packages.sh


.PHONY: release
release:
	@[ "$$(git branch --show-current)" = "main" ] || { echo "ERROR: must be on main branch"; exit 1; }
	@VERSION=$$(grep '^version' pyproject.toml | head -1 | sed 's/.*"\(.*\)"/\1/'); \
	[ -z "$$(git tag -l $$VERSION)" ] || { echo "ERROR: tag $$VERSION already exists"; exit 1; }; \
	echo "Creating release tag $$VERSION..."; \
	git tag -a $$VERSION -m "Release version $$VERSION" && \
	git push origin tag $$VERSION && \
	echo "Released $$VERSION"


.PHONY: docs
docs:
	cd docs && uv run make clean api html


.PHONY: docs
pub-docs:
	bin/publish-docs.sh


.PHONY: clean
clean:
	rm -rf build
	rm -rf dist
	rm -rf *.egg*/
	find redun -type f -name '*.pyc' -delete


.PHONY: teardown
teardown:
	rm -rf .venv/
	rm -rf .tox/
