import os

from redun.config import Config
from redun.executors.code_packaging import (
    create_tar,
    extract_tar,
    find_code_files,
    package_code,
    parse_code_package_config,
)
from redun.file import Dir, File
from redun.tests.utils import use_tempdir


@use_tempdir
def test_find_code_files():
    # Creating python files.
    File("workflow.py").write("")
    File("lib/lib.py").write("")
    File("lib/module/lib.py").write("")

    # Create unrelated files.
    File("unrelated.txt").write("")
    File("lib/unrelated.txt").write("")

    # Create python files in hidden directories.
    File(".venv/lib.py").write("")

    # Create python files we want excluded.
    File("lib2/module/lib.py").write("")

    files = find_code_files()
    assert files == {
        "./workflow.py",
        "./lib/lib.py",
        "./lib/module/lib.py",
        "./lib2/module/lib.py",
    }

    files = find_code_files(excludes=["lib2/**/**"])
    assert files == {"./workflow.py", "./lib/lib.py", "./lib/module/lib.py"}

    files = find_code_files(includes=["lib/**/**.py", "lib2/**/**.py"])
    assert files == {"./lib/lib.py", "./lib/module/lib.py", "./lib2/module/lib.py"}


@use_tempdir
def test_tar_code_files():
    # Creating python files.
    File("workflow.py").write("")
    File("lib/lib.py").write("")
    File("lib/module/lib.py").write("")

    # Create unrelated files.
    File("unrelated.txt").write("")
    File("lib/unrelated.txt").write("")

    # Create python files in hidden directories.
    File(".venv/lib.py").write("")

    # Create python files we want excluded.
    File("lib2/module/lib.py").write("")

    tar_path = "code.tar.gz"
    file_paths = find_code_files()
    tar_file = create_tar(tar_path, file_paths)

    os.makedirs("dest")
    extract_tar(tar_file, "dest")

    files2 = {file.path for file in Dir("dest")}
    assert files2 == {
        "dest/lib/module/lib.py",
        "dest/workflow.py",
        "dest/lib2/module/lib.py",
        "dest/lib/lib.py",
    }


@use_tempdir
def test_package_job_code() -> None:
    """
    package_code() should include the right files and use the right tar filename.
    """

    # Creating python files.
    File("workflow.py").write("")
    File("lib/lib.py").write("")
    File("lib/module/lib.py").write("")

    # Create unrelated files.
    File("unrelated.txt").write("")
    File("lib/unrelated.txt").write("")

    # Create python files in hidden directories.
    File(".venv/lib.py").write("")

    # Create python files we want excluded.
    File("lib2/module/lib.py").write("")

    # Package up code.
    s3_scratch_prefix = "s3/"
    code_package = {"include": ["**/*.py"]}
    code_file = package_code(s3_scratch_prefix, code_package)

    # Code file should have the right path.
    assert code_file.path.startswith(os.path.join(s3_scratch_prefix, "code"))
    assert code_file.path.endswith(".tar.gz")

    # code_file should contain the right files.
    os.makedirs("dest")
    extract_tar(code_file, "dest")

    files = {file.path for file in Dir("dest")}
    assert files == {
        "dest/lib/module/lib.py",
        "dest/workflow.py",
        "dest/lib2/module/lib.py",
        "dest/lib/lib.py",
    }


def test_parse_code_package_config():
    # Parse default code_package patterns.
    config = Config({"batch": {}})
    assert parse_code_package_config(config["batch"]) == {"excludes": [], "includes": ["**/*.py"]}

    # Disable code packaging.
    config = Config({"batch": {"code_package": False}})
    assert parse_code_package_config(config["batch"]) is False

    # Custom include exclude.
    config = Config({"batch": {"code_includes": "**/*.txt", "code_excludes": ".venv/**"}})
    assert parse_code_package_config(config["batch"]) == {
        "includes": ["**/*.txt"],
        "excludes": [".venv/**"],
    }

    # Multiple patterns with special chars.
    config = Config(
        {"batch": {"code_includes": '**/*.txt "my file.txt" *.py', "code_excludes": ".venv/**"}}
    )
    assert parse_code_package_config(config["batch"]) == {
        "includes": ["**/*.txt", "my file.txt", "*.py"],
        "excludes": [".venv/**"],
    }
