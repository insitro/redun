import re
from pathlib import Path

import redun


def test_version_matches_pyproject():
    """
    Ensure redun._version.__version__ matches the version in pyproject.toml.

    This test prevents the hardcoded version from falling out of sync with
    the package version specified in pyproject.toml.
    """
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
    pyproject_content = pyproject_path.read_text()

    # Extract version from pyproject.toml
    match = re.search(r'^version\s*=\s*"([^"]+)"', pyproject_content, re.MULTILINE)
    assert match, "Could not find version in pyproject.toml"
    pyproject_version = match.group(1)

    assert redun.__version__ == pyproject_version, (
        f"Version mismatch: redun._version.__version__ is {redun.__version__!r} "
        f"but pyproject.toml has {pyproject_version!r}. "
        "Please update redun/_version.py to match pyproject.toml."
    )


def test_version_alias():
    """Ensure redun.version is an alias for redun.__version__."""
    assert redun.version == redun.__version__


def test_version_matches_metadata():
    """Ensure redun.__version__ matches importlib.metadata.version('redun')."""
    from importlib.metadata import version as metadata_version

    assert redun.__version__ == metadata_version("redun"), (
        f"Version mismatch: redun.__version__ is {redun.__version__!r} "
        f"but importlib.metadata.version('redun') returns {metadata_version('redun')!r}."
    )
