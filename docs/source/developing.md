# Developer guide

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

1. Prepare a release branch and make a PR from this branch with an updated [`__version__`](https://github.com/insitro/redun/blob/db17e39a2efaf9b3be466c60bdfecbe6ce4ea054/redun/__init__.py#L14) and [`CHANGELOG`](https://github.com/insitro/redun/blob/master/docs/source/CHANGELOG.rst)
    e.g. https://github.com/insitro/redun/pull/107. Use [`release_notes.py`](docs/release_notes.py) script to generate the release notes (see that script for usage).
2. Merge the release branch. The codebuild pipeline will trigger automatically. If the version has been updated, it will initiate the release process. If the CHANGELOG is not updated to reflect the new version, the codebuild will terminate without releasing.
