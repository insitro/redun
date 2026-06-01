import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
SCRIPT = REPO_ROOT / "bin" / "extract-changelog-section.sh"
CHANGELOG = REPO_ROOT / "docs" / "source" / "CHANGELOG.md"


def run_script(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_happy_path_extracts_known_version():
    result = run_script("0.46.0", str(CHANGELOG))
    assert result.returncode == 0
    assert "#508" in result.stdout
    assert "#507" in result.stdout
    assert "#506" in result.stdout
    assert "## 0.45.0" not in result.stdout
    assert "May 15, 2026" in result.stdout


def test_first_version_in_file_does_not_bleed():
    """The most recent version (first ## section) should extract only its own bullets."""
    result = run_script("0.46.0", str(CHANGELOG))
    assert result.returncode == 0
    # Should not include any earlier version's content
    assert "#504" not in result.stdout  # 0.45.0
    assert "#496" not in result.stdout  # 0.44.2


def test_last_version_extracts_to_eof():
    """The oldest version in the file should extract without bleeding past EOF."""
    # Find the last version header in the file.
    content = CHANGELOG.read_text().splitlines()
    last_version = None
    for line in content:
        if line.startswith("## ") and len(line) > 3 and line[3].isdigit():
            last_version = line[3:].strip()
    assert last_version is not None, "No version header found in CHANGELOG"

    result = run_script(last_version, str(CHANGELOG))
    assert result.returncode == 0
    assert result.stdout, "Last version extraction should produce non-empty output"


def test_missing_version_returns_empty():
    result = run_script("99.99.99", str(CHANGELOG))
    assert result.returncode == 0
    assert result.stdout == ""


def test_missing_changelog_file_exits_with_error():
    result = run_script("0.46.0", "/nonexistent/path/CHANGELOG.md")
    assert result.returncode == 1
    assert "changelog not found" in result.stderr


def test_wrong_arg_count_shows_usage():
    result = subprocess.run(
        [str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "usage:" in result.stderr

    result = subprocess.run(
        [str(SCRIPT), "0.46.0"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "usage:" in result.stderr


def test_section_boundary_is_exact_match():
    """Version '0.4' must not match '## 0.40.0' header (exact-line awk match)."""
    result = run_script("0.4", str(CHANGELOG))
    assert result.returncode == 0
    # If exact match works, '0.4' isn't a real header so output is empty.
    # If awk did substring matching, it would extract 0.40.0's content.
    assert "#472" not in result.stdout, "Substring match leaked from 0.40.0"
