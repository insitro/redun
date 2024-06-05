import glob
import os
import shlex
import tarfile
import tempfile
import zipfile
from typing import Iterable, List, Optional, Set, Union

from redun.executors.scratch import get_code_scratch_file
from redun.file import File
from redun.hashing import hash_stream


def find_code_files(
    basedir: str = ".",
    includes: Optional[List[str]] = None,
    excludes: Optional[List[str]] = None,
) -> Iterable[str]:
    """
    Find all the workflow code files consistent with the include/exclude patterns.
    """
    if includes is None:
        includes = ["**/*.py"]
    if excludes is None:
        excludes = []

    files: Set[str] = set()
    for pattern in includes:
        files |= set(glob.glob(os.path.join(basedir, pattern), recursive=True))

    for pattern in excludes:
        files -= set(glob.glob(os.path.join(basedir, pattern), recursive=True))
    return files


def create_tar(
    tar_path: str, file_paths: Iterable[str], arcname_prefix: Optional[str] = None
) -> File:
    """
    Create a tar file from local file paths.
    Args:
        arcname_prefix: prefix to add to each file path in the tar file.
    """
    tar_file = File(tar_path)

    with tar_file.open("wb") as out:
        with tarfile.open(fileobj=out, mode="w|gz") as tar:
            for file_path in file_paths:
                # add prefix to each file
                arcname = os.path.join(arcname_prefix, file_path) if arcname_prefix else file_path
                tar.add(file_path, arcname=arcname)

    return tar_file


def extract_tar(tar_file: File, dest_dir: str = ".") -> None:
    """
    Extract a tar file to local paths.
    """
    with tar_file.open("rb") as infile:
        with tarfile.open(fileobj=infile, mode="r|gz") as tar:
            tar.extractall(dest_dir)


def create_zip(
    zip_path: str,
    base_path: str,
    file_paths: Iterable[str],
    arcname_prefix: Optional[str] = None,
) -> File:
    """
    Create a zip file from local file paths.
    Args:
        arcname_prefix: prefix to add to each file path in the tar file.
    """
    zip_file = File(zip_path)

    with zip_file.open("wb") as out:
        with zipfile.ZipFile(out, mode="w") as stream:
            for file_path in file_paths:
                arcname = os.path.relpath(file_path, base_path)
                arcname = os.path.join(arcname_prefix, arcname) if arcname_prefix else arcname
                stream.write(file_path, arcname)

    return zip_file


def parse_code_package_config(config) -> Union[dict, bool]:
    """
    Parse the code package options from a AWSBatchExecutor config.
    """
    if not config.getboolean("code_package", fallback=True):
        return False

    include_config = config.get("code_includes", "**/*.py")
    exclude_config = config.get("code_excludes", "")

    return {
        "includes": shlex.split(include_config),
        "excludes": shlex.split(exclude_config),
    }


def package_code(
    scratch_prefix: str,
    code_package: dict = {},
    use_zip: bool = False,
    basename: Optional[str] = None,
    arcname_prefix: Optional[str] = None,
) -> File:
    """
    Package code to scratch directory.
    Args:
        basename: If provided, uses this string as the basename instead of the
            calculated tarball hash.
        arcname_prefix: Optional suffix to append to tarball basename.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        file_paths = find_code_files(
            includes=code_package.get("includes"), excludes=code_package.get("excludes")
        )

        if use_zip:
            temp_file = File(os.path.join(tmpdir, "code.zip"))
            create_zip(temp_file.path, ".", file_paths, arcname_prefix=arcname_prefix)
        else:
            temp_file = File(os.path.join(tmpdir, "code.tar.gz"))
            create_tar(temp_file.path, file_paths, arcname_prefix=arcname_prefix)

        if not basename:
            with temp_file.open("rb") as infile:
                basename = hash_stream(infile)

        code_file = File(get_code_scratch_file(scratch_prefix, basename, use_zip=use_zip))
        if not code_file.exists():
            temp_file.copy_to(code_file)

    return code_file
