---
tocpdeth: 3
---

# Developer guide

## Development installation

If you have postgres available, (perhaps with `brew install postgresql` on macOS), you can prepare
an editable installation and run the tests:
```shell
make setup
. .venv/bin/activate
pytest redun

# Other targets
make lint
make format
```

## Release

Redun releases are done via the `redun-release-auto` [codebuild pipeline](https://us-west-2.console.aws.amazon.com/codesuite/codebuild/projects/redun-release-auto/).

### Release Steps

1. Prepare a release branch and make a PR from this branch with an updated:
   - [`version`](https://github.com/insitro/redun/blob/main/pyproject.toml) in `pyproject.toml` - new version number should follow semantic versioning rules (see [semver.org](https://semver.org/) for details)
   - [`CHANGELOG`](https://github.com/insitro/redun/blob/main/docs/source/CHANGELOG.md) - use [`release_notes.py`](docs/release_notes.py) script to generate the release notes (see that script for usage)
   - `uv.lock` - run `uv lock` to update the lockfile after changing the version
2. Merge the release branch to both branches `main` and `insitro-private`. The codebuild pipeline will trigger automatically based on the latter branch. If the version has been updated, it will initiate the release process. If the CHANGELOG is not updated to reflect the new version, the codebuild will terminate without releasing.
