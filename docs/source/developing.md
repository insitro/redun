---
tocpdeth: 3
---

# Developer guide

## One-liner to run tests using Docker & Docker-Compose

Run `./pytest.sh` with the arguments that you would pass to `pytest`, for example:

```
./pytest.sh -k static --pdb
```

The lints also have their own shortcut: `./lint.sh`

## Development installation

If you have postgres available, (perhaps with `brew install postgresql` on macOS), you can prepare
an editable installation and run the tests:
```shell
make setup
. .venv/bin/activate
pytest redun

# Other targets
make black
make mypy
make lint
make isort
```

## Release

Redun releases are done via the `redun-release-auto` [codebuild pipeline](https://us-west-2.console.aws.amazon.com/codesuite/codebuild/projects/redun-release-auto/).

### Release Steps

1. Prepare a release branch and make a PR from this branch with an updated:
   - [`__version__`](https://github.com/insitro/redun/blob/0cd06c8147700f67b777b5a43a6d3e3925274bff/redun/__init__.py#L21) - new version number should follow semantic versioning rules (see [semver.org](https://semver.org/) for details)
   - and [`CHANGELOG`](https://github.com/insitro/redun/blob/main/docs/source/CHANGELOG.md) e.g. https://github.com/insitro/redun/pull/107. Use [`release_notes.py`](docs/release_notes.py) script to generate the release notes (see that script for usage).
2. Merge the release branch to both branches `main` and `insitro-private`. The codebuild pipeline will trigger automatically based on the latter branch. If the version has been updated, it will initiate the release process. If the CHANGELOG is not updated to reflect the new version, the codebuild will terminate without releasing.
