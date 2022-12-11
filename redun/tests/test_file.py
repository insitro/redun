import os
import shlex
import shutil
from unittest.mock import patch

import boto3
import pytest
import vcr

import redun.file
from redun import File, Scheduler, ShardedS3Dataset, task
from redun.file import (
    ContentDir,
    ContentFile,
    ContentStagingFile,
    Dir,
    IDir,
    IFile,
    IStagingFile,
    StagingDir,
    StagingFile,
    get_filesystem,
    glob_file,
)
from redun.tests.utils import mock_s3, use_tempdir
from redun.tools import copy_file
from redun.value import get_type_registry


@use_tempdir
def test_file_api() -> None:
    file = File("hello.txt")
    file.remove()

    assert not file.exists()

    # Removing a file that doesn't exist should be safe.
    file.remove()

    with file.open("w") as out:
        out.write("hello")

    assert file.exists()

    assert file.read() == "hello"

    hash = file.get_hash()

    with file.open("w") as out:
        out.write("hello2")

    assert file.get_hash() != hash

    file2 = File("hello2.txt")
    assert file.copy_to(file2) == file2
    assert file2.read() == "hello2"

    assert file.size() == 6


@use_tempdir
def test_encoding() -> None:
    """
    File read/write/open should support encodings.
    """
    text = "hello \u1F600"

    # Read and write utf-8.
    file = File("hello.txt")
    file.write(text, encoding="utf-8")
    assert file.read(encoding="utf-8") == text
    assert file.read("rb") == b"hello \xe1\xbd\xa00"

    # Read and write utf-16.
    file2 = File("hello.txt")
    file2.write(text, encoding="utf-16")
    assert file2.read(encoding="utf-16") == text
    assert file2.read("rb") == b"\xff\xfeh\x00e\x00l\x00l\x00o\x00 \x00`\x1f0\x00"

    # Ensure File context can use encoding.
    with file.open(mode="w", encoding="utf-8") as out:
        out.write(text)

    with file.open(mode="r", encoding="utf-8") as infile:
        assert infile.read() == text

    # Binary mode cannot be used with encoding.
    with pytest.raises(ValueError, match="encoding"):
        file.open(mode="rb", encoding="utf-8")


@use_tempdir
def test_dir_api() -> None:

    dir = Dir("hi")
    assert dir.rel_path(os.getcwd()) == ".."

    assert not dir.exists()
    dir.mkdir()
    assert dir.exists()

    f = dir.file("h")
    f.touch()
    assert not dir.is_valid()

    with pytest.raises(OSError):
        dir.rmdir(recursive=False)
    assert dir.exists()

    dir.rmdir(recursive=True)
    assert not dir.exists()
    assert not f.exists()


@use_tempdir
def test_file_subvalues() -> None:
    """
    Files should be discovered as subvalues of a nested value.
    """
    file1 = File("file1")
    file2 = File("file2")
    file3 = File("file3")

    value = [file1, {"a": file2, "b": file3}]
    registry = get_type_registry()
    subvalues = set(registry.iter_subvalues(value))
    assert subvalues == {file1, file2, file3}


@use_tempdir
def test_dir_subvalues() -> None:
    """
    Files should be discovered as subvalues of a Dir.
    """
    file1 = File("dir/file1")
    file1.write("a")
    file2 = File("dir/file2")
    file2.write("a")
    file3 = File("dir/file3")
    file3.write("a")

    dir = Dir("dir")

    # Ensure Dir contains our files.
    file_hashes = {file.hash for file in dir}
    assert file_hashes == {file1.hash, file2.hash, file3.hash}

    registry = get_type_registry()
    subvalues = set(registry.iter_subvalues(dir))
    subvalue_hashes = {subvalue.get_hash() for subvalue in subvalues}
    assert subvalue_hashes == {file1.hash, file2.hash, file3.hash}


@use_tempdir
def test_workflow(scheduler: Scheduler) -> None:

    task_calls = []

    @task()
    def task1(out_path):
        task_calls.append("task1")
        file = File(out_path)
        with file.open("w") as out:
            out.write("hello")
        return file

    @task()
    def task2(infile):
        return "task2({})".format(infile.read())

    @task()
    def workflow():
        file = task1("out.txt")
        return file

    file = scheduler.run(workflow())

    assert task_calls == ["task1"]
    assert file.is_valid()
    assert file.read() == "hello"

    # Rerun a cached workflow.
    file = scheduler.run(workflow())

    assert task_calls == ["task1"]
    assert file.is_valid()
    assert file.read() == "hello"

    # Alter file out-of-band.
    with open(file.path, "w") as out:
        out.write("hello2")
    assert not file.is_valid()

    # Workflow reruns.
    file = scheduler.run(workflow())
    assert file.read() == "hello"
    assert task_calls == ["task1", "task1"]


@use_tempdir
def test_infile(scheduler: Scheduler) -> None:

    task_calls = []

    @task()
    def task1(out_path):
        task_calls.append("task1")
        file = File(out_path)
        with file.open("w") as out:
            out.write("hello")
        return file

    @task()
    def task2(infile):
        task_calls.append("task2")
        return "task2({})".format(infile.read())

    @task()
    def workflow():
        file = task1("out.txt")
        return task2(file)

    data = scheduler.run(workflow())
    assert data == "task2(hello)"
    assert task_calls == ["task1", "task2"]

    data = scheduler.run(workflow())
    assert data == "task2(hello)"
    assert task_calls == ["task1", "task2"]


@use_tempdir
def test_new_file(scheduler: Scheduler) -> None:
    @task()
    def task1(infile):
        return "task1({})".format(infile.read())

    @task()
    def workflow(file):
        return task1(file)

    file = File("out.txt")
    file.write("hello")

    data = scheduler.run(workflow(File("out.txt")))
    assert data == "task1(hello)"

    file.write("hello2")

    data = scheduler.run(workflow(File("out.txt")))
    assert data == "task1(hello2)"


def test_is_valid() -> None:
    """
    File should not be valid if recorded hash does not match current file hash.
    """
    new_file = File("new_file")
    new_file._hash = "123"
    assert not new_file.is_valid()


@use_tempdir
def test_file_hash() -> None:
    file = File("hello.txt")
    file.write("hello")
    assert file.is_valid()


def test_glob() -> None:
    expected = [
        "redun/tests/test_data/file_glob/",
        "redun/tests/test_data/file_glob/aa",
        "redun/tests/test_data/file_glob/aa/bb",
        "redun/tests/test_data/file_glob/aa/bb/cc.txt",
        "redun/tests/test_data/file_glob/aa/bb/dd.txt",
        "redun/tests/test_data/file_glob/aa/zz.txt",
    ]
    assert sorted(glob_file("redun/tests/test_data/file_glob/**")) == expected


@use_tempdir
def test_dir() -> None:
    src_dir = os.path.join(os.path.dirname(__file__), "test_data")

    path = "base"
    shutil.copytree(src_dir, path)

    # Directory should be valid initially.
    dir = Dir(path)
    assert dir.is_valid()

    # Adding a new file should cause the Dir to not be valid anymore.
    File(path + "/qqq.txt").touch()
    assert not dir.is_valid()


@use_tempdir
def test_check_valid_result(scheduler: Scheduler) -> None:
    file = File("output")
    file.write("hello")

    @task()
    def task1():
        return File("output")

    @task()
    def task2():
        return task1().read()

    @task()
    def main(check_valid="full"):
        return task2.options(check_valid=check_valid)()

    assert scheduler.run(main()) == "hello"

    file.write("hello2")

    # The result of task1, which is deeper down the call stack, will have
    # its result checked, and be re-evaluated.
    assert scheduler.run(main()) == "hello2"

    # Using valid_check=shallow, we don't re-evaluate task1.
    file.write("hello3")
    assert scheduler.run(main("shallow")) == "hello2"


@use_tempdir
def test_check_valid_args() -> None:
    file = File("input")
    file.write("hello")

    @task()
    def task1(file):
        return file.read()

    @task()
    def task2():
        file = File("input")
        return task1(file)

    @task()
    def main(check_valid="full"):
        return task2.options(check_valid=check_valid)()

    scheduler = Scheduler()
    assert scheduler.run(main()) == "hello"

    file.write("hello2")

    # The result of task1, which is deeper down the call stack, will have
    # its result checked, and be re-evaluated.
    assert scheduler.run(main()) == "hello2"

    # Create a new scheduler, with new cache.
    scheduler = Scheduler()
    assert scheduler.run(main("shallow")) == "hello2"

    # Using valid_check=shallow, we don't re-evaluate task1.
    file.write("hello3")
    assert scheduler.run(main("shallow")) == "hello2"


def test_staging_file() -> None:
    """
    StagingFile constructor should support paths and Files.
    """

    # Path-based constructor.
    staging_file = StagingFile("local.txt", "remote.txt")
    assert isinstance(staging_file.local, File)
    assert isinstance(staging_file.remote, File)
    assert staging_file.local.path == "local.txt"
    assert staging_file.remote.path == "remote.txt"

    # File-based constructor.
    staging_file2 = StagingFile(File("local.txt"), File("remote.txt"))
    assert isinstance(staging_file2.local, File)
    assert isinstance(staging_file2.remote, File)
    assert staging_file2.local.path == "local.txt"
    assert staging_file2.remote.path == "remote.txt"


def test_file_stage() -> None:
    """
    File.stage() should produce a StagingFile.
    """

    file = File("remote/file.txt")
    staging_file = file.stage("local/file.txt")
    assert staging_file.local.path == "local/file.txt"
    assert staging_file.remote.path == "remote/file.txt"

    staging_file = file.stage("local/")
    assert staging_file.local.path == "local/file.txt"

    staging_file = file.stage()
    assert staging_file.local.path == "file.txt"


@use_tempdir
def test_staging_self() -> None:
    """
    StagingFile/StagingDir should allow staging to itself.
    """
    file = File("file.txt")
    file.write("hello")

    staging_file = file.stage("file.txt")
    assert staging_file.render_stage() == ""
    assert staging_file.render_unstage() == ""

    file2 = staging_file.stage()
    assert file2.hash == file.hash

    file2 = staging_file.unstage()
    assert file2.hash == file.hash

    dir = Dir("dir")
    dir.file("a").write("a")
    dir.file("b").write("b")
    dir.mkdir()

    staging_dir = dir.stage("dir")
    assert staging_dir.render_stage() == ""
    assert staging_dir.render_unstage() == ""

    dir2 = staging_dir.stage()
    assert dir2.hash == dir.hash

    dir2 = staging_dir.unstage()
    assert dir2.hash == dir.hash


def test_staging_dir() -> None:
    """
    StagingDir constructor should support paths and Files.
    """

    # Path-based constructor.
    staging_dir = StagingDir("local", "remote")
    assert isinstance(staging_dir.local, Dir)
    assert isinstance(staging_dir.remote, Dir)
    assert staging_dir.local.path == "local"
    assert staging_dir.remote.path == "remote"

    # File-based constructor.
    staging_dir2 = StagingDir(Dir("local"), Dir("remote"))
    assert isinstance(staging_dir2.local, Dir)
    assert isinstance(staging_dir2.remote, Dir)
    assert staging_dir2.local.path == "local"
    assert staging_dir2.remote.path == "remote"


def test_dir_stage() -> None:
    """
    Dir.stage() should produce a StagingDir.
    """

    dir = Dir("remote/dir")
    staging_dir = dir.stage("local/dir")
    assert staging_dir.local.path == "local/dir"
    assert staging_dir.remote.path == "remote/dir"

    staging_dir = dir.stage()
    assert staging_dir.local.path == "dir"


@use_tempdir
def test_staging_file_value() -> None:
    """
    StagingFile should implement the Value interface.
    """
    remote = File("remote.txt")
    staging_file = remote.stage("local.txt")
    assert isinstance(staging_file, StagingFile)
    assert StagingFile.type_name == "redun.StagingFile"

    # StagingFile should satisfy the Value interface.
    assert staging_file.get_hash() == "cfb6d4d9de4d45ed80bbdb6d411ffc22618b9db1"

    data = staging_file.serialize()
    staging_file2 = StagingFile.deserialize(StagingFile, data)

    assert staging_file2.remote.path == "remote.txt"
    assert staging_file2.local.path == "local.txt"

    assert staging_file.is_valid()

    # The hash of a StagingFile should only depend on the paths of the File pair,
    # not on the contents (hash) of the Files.
    remote.write("hello")
    assert staging_file.get_hash() == "cfb6d4d9de4d45ed80bbdb6d411ffc22618b9db1"


@mock_s3
@use_tempdir
def test_sharded_dataset_from_file() -> None:
    # Create a S3 bucket with a few files.
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    # Raise an error if files don't exist
    with pytest.raises(FileNotFoundError):
        _ = ShardedS3Dataset.from_files(
            [
                File("s3://example-bucket/bar/baz.csv"),
                File("s3://example-bucket/x/y.csv"),
                File("s3://example-bucket/bar/baz/baz2.csv"),
            ]
        )

    file1 = File("s3://example-bucket/data/more_data/shard1.csv")
    file2 = File("s3://example-bucket/data/other_data/shard2.csv")
    file3 = File("s3://example-bucket/data/shard3.csv")
    file4 = File("s3://example-bucket/data/some.parquet")
    file1.write("word1, word2\nhello, world")
    file2.write("word1, word2\ncarrots, potatoes")
    file3.write("word1, word2\nfoo, bar")
    file4.write("well it looks like a parquet file")

    # Test a single file
    x = ShardedS3Dataset.from_files([file1], allow_additional_files=True)
    assert x.path == "s3://example-bucket/data/more_data"
    assert x.format == "csv"

    # Test incompatible file formats.
    with pytest.raises(ValueError):
        _ = ShardedS3Dataset.from_files([file1, file2, file3, file4])

    # Test files not on S3
    local_file = File("./potato.csv")
    local_file.write("i like potato")
    with pytest.raises(ValueError):
        _ = ShardedS3Dataset.from_files([local_file])

    # Test empty file list
    with pytest.raises(ValueError):
        _ = ShardedS3Dataset.from_files([])

    file4.remove()

    # Making a dataset that would contain additional files (in this case file3)
    # should raise an error.
    with pytest.raises(ValueError):
        _ = ShardedS3Dataset.from_files([file1, file2])

    x = ShardedS3Dataset.from_files([file1, file2], allow_additional_files=True)
    assert len(x.filenames) == 3
    assert file3.path in x.filenames
    assert x.format == "csv"
    assert x.path == "s3://example-bucket/data"

    x = ShardedS3Dataset.from_files([file1, file2, file3])
    assert len(x.filenames) == 3
    assert file3.path in x.filenames
    assert x.format == "csv"
    assert x.path == "s3://example-bucket/data"


@mock_s3
def test_sharded_dataset() -> None:
    """
    Tests ShardedS3Dataset hashing
    """
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    # We have 2 shards, 1 partition with another shard, and a txt file.
    file1 = File("s3://example-bucket/data/shard1.csv")
    file1.write("word1, word2\nhello, world")
    file2 = File("s3://example-bucket/data/shard2.csv")
    file2.write("word1, word2\ncarrots, potatoes")
    file3 = File("s3://example-bucket/data/word1=foo/shard1.csv")
    file3.write("word1, word2\nfoo, bar")
    decoy = File("s3://example-bucket/data/not_a_csv.txt")
    decoy.write("i am not valid input")

    dataset = ShardedS3Dataset(path="s3://example-bucket/data", format="csv", recurse=True)
    dataset2 = ShardedS3Dataset(path="s3://example-bucket/data", format="csv", recurse=True)

    # Check the correct files are pulled in.
    assert sorted(dataset.filenames) == sorted([file1.path, file2.path, file3.path])
    assert sorted(dataset2.filenames) == sorted([file1.path, file2.path, file3.path])

    # Update the recurse value and ensure the list of files is updated.
    dataset.recurse = False
    assert sorted(dataset.filenames) == sorted([file1.path, file2.path])

    # Check hashing changes when list of files changes.
    assert dataset.hash == "3394197d206ea0ef46795131b98f86c52ab9a508"
    assert dataset2.hash == "f7cd6b0188ff900f0ca00ea0b937a91d70a3e67a"

    # Check type registry uses the correct hash function.
    assert dataset.hash == get_type_registry().get_hash(dataset)

    file3.remove()
    assert dataset.is_valid()
    assert not dataset2.is_valid()

    # Changing the path should update the list of files.
    dataset2.path = "s3://example-bucket/nonexistent"
    assert dataset2.filenames == []

    # Changing the format should as well.
    dataset2.path = "s3://example-bucket/data"
    assert len(dataset2.filenames)
    dataset2.format = "parquet"
    assert dataset2.filenames == []

    # Load should not work without glue context.
    with pytest.raises(ValueError):
        _ = dataset.load_spark()

    # Try loading one shard.
    dat = dataset.load_pandas_shards(max_shards=1)
    assert len(dat) == 1
    assert isinstance(dat, list)

    # Now load the whole dataset.
    concatdat = dataset.load_pandas()
    assert concatdat[concatdat["word1"] == "carrots"].shape == (1, 2)
    assert list(concatdat["word1"].sort_values()) == ["carrots", "hello"]


@use_tempdir
def test_staging_dir_value() -> None:
    """
    StagingDir should implement the Value interface.
    """
    remote = Dir("remote")
    staging_dir = remote.stage("local")
    assert isinstance(staging_dir, StagingDir)
    assert StagingDir.type_name == "redun.StagingDir"

    # StagingDir should satisfy the Value interface.
    assert staging_dir.get_hash() == "bb933a6873e68cdb47ecf5af8c5784608966ae14"

    data = staging_dir.serialize()
    staging_dir2 = StagingDir.deserialize(StagingDir, data)

    assert staging_dir2.remote.path == "remote"
    assert staging_dir2.local.path == "local"

    assert staging_dir.is_valid()

    # The hash of a StagingDir should only depend on the paths of the File pair,
    # not on the contents (hash) of the Files.
    File("remote/file").write("hello")
    assert staging_dir.get_hash() == "bb933a6873e68cdb47ecf5af8c5784608966ae14"


@use_tempdir
@mock_s3
def test_staging_render() -> None:
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    file = File("s3://example-bucket/remote.txt")
    file.write("hello")

    # Ensure StagingFile is setup correctly.
    staging_file = file.stage("local.txt")
    assert isinstance(staging_file, StagingFile)
    assert staging_file.remote.path == "s3://example-bucket/remote.txt"
    assert staging_file.local.path == "local.txt"

    # Stage a file.
    assert not staging_file.local.exists()
    staging_file.stage()
    assert staging_file.local.read() == "hello"

    # Unstage a file.
    staging_file.local.write("hello2")
    staging_file.unstage()
    assert staging_file.remote.read() == "hello2"

    # S3 staging commands.
    assert (
        staging_file.render_stage()
        == "aws s3 cp --no-progress s3://example-bucket/remote.txt local.txt"
    )
    assert (
        staging_file.render_unstage()
        == "aws s3 cp --no-progress local.txt s3://example-bucket/remote.txt"
    )

    # Local filesystem staging commands.
    file2 = File("remote.txt")
    staging_file2 = file2.stage("local.txt")
    assert staging_file2.render_stage() == "cp remote.txt local.txt"
    assert staging_file2.render_unstage() == "cp local.txt remote.txt"


def test_staging_render_gs() -> None:
    """
    Should be able to render stage commands for gs.
    """
    cmd = File("gs://bucket/remote.txt").stage("local.txt").render_stage()
    assert cmd == "gsutil cp gs://bucket/remote.txt local.txt"

    cmd = File("gs://bucket/remote.txt").stage("local.txt").render_unstage()
    assert cmd == "gsutil cp local.txt gs://bucket/remote.txt"


def test_render_stage_quote() -> None:
    file = File("s3://example-bucket/crazy name \" \\ ' .txt")
    stage_file = file.stage()
    command = stage_file.render_stage()
    assert command == (
        r"""aws s3 cp --no-progress 's3://example-bucket/crazy name " \ '"'"' .txt' 'crazy name " \ '"'"' .txt'"""  # noqa: E501
    )
    assert shlex.split(command) == [
        "aws",
        "s3",
        "cp",
        "--no-progress",
        stage_file.remote.path,
        stage_file.local.path,
    ]


@use_tempdir
@mock_s3
def test_staging_dir_render() -> None:
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="example-bucket")

    File("s3://example-bucket/dir/a.txt").write("a")
    File("s3://example-bucket/dir/b.txt").write("b")
    File("s3://example-bucket/dir/c/d.txt").write("d")

    # Ensure StagingDir is setup correctly.
    dir = Dir("s3://example-bucket/dir")
    staging_dir = dir.stage("local")

    assert isinstance(staging_dir, StagingDir)
    assert staging_dir.remote.path == "s3://example-bucket/dir"
    assert staging_dir.local.path == "local"

    # Stage a dir.
    assert not staging_dir.local.exists()
    staging_dir.stage()
    set(staging_dir.local) == {
        "s3://example-bucket/dir/a.txt",
        "s3://example-bucket/dir/b.txt",
        "s3://example-bucket/dir/c/d.txt",
    }

    # Unstage a dir.
    staging_dir.local.file("dir/a.txt").write("hello2")
    staging_dir.unstage()
    staging_dir.remote.file("dir/a.txt").read() == "hello2"

    # S3 staging commands.
    assert (
        staging_dir.render_stage()
        == "aws s3 cp --no-progress --recursive s3://example-bucket/dir local"
    )
    assert (
        staging_dir.render_unstage()
        == "aws s3 cp --no-progress --recursive local s3://example-bucket/dir"
    )

    # Local filesystem staging commands.
    dir2 = Dir("remote")
    staging_dir2 = dir2.stage("local")
    assert staging_dir2.render_stage() == "cp -r remote local"
    assert staging_dir2.render_unstage() == "cp -r local remote"


@use_tempdir
def test_copy_file_task(scheduler: Scheduler) -> None:
    """
    copy_file should copy a file from one location to another.
    """
    src_file = File("data.txt")
    src_file.write("hello")

    dest_file = scheduler.run(copy_file(src_file, "data2.txt"))
    assert dest_file.read() == "hello"


@vcr.use_cassette("redun/tests/test_data/vcr_cassettes/test_https.yaml")
@use_tempdir
def test_https() -> None:
    """
    File should work with https URLs.
    """
    url = "https://www.google.com/robots.txt"
    expected_data = "User-agent"

    # Use FileSystem for https protocol.
    fs = get_filesystem(proto="https")
    assert fs.open(url, "r").read(10) == expected_data

    # Open and read a File over https.
    file = File(url)
    with file.open() as infile:
        assert infile.read(10) == expected_data

    # https Files should support hashing.
    assert file.get_hash() == "106920f9bb975ad607f701992c6ca855f9309c96"

    # Should support file size.
    assert file.size() > 10

    # We should be able to copy from https to local files.
    local_file = File("robots.txt")
    file.copy_to(local_file)
    assert local_file.open().read(10) == expected_data

    # Other File methods should work as expected.
    assert fs.isfile(url)
    assert not fs.isdir(url)


def test_shell_copy_local() -> None:
    """
    LocalFileSystem should be able to generate shell commands for file copies.
    """
    assert File("src.txt").shell_copy_to("dest.txt") == "cp src.txt dest.txt"
    assert File("src.txt").shell_copy_to("dest space.txt") == "cp src.txt 'dest space.txt'"
    assert File("src.txt").shell_copy_to(None) == "cat src.txt"
    assert get_filesystem("local").shell_copy(None, "dest.txt") == "cat > dest.txt"

    assert get_filesystem("local").shell_copy("src", "dest", recursive=True) == "cp -r src dest"


def test_shell_copy_s3() -> None:
    """
    S3FileSystem should be able to generate shell commands for file copies.
    """
    assert (
        File("s3://bucket/src.txt").shell_copy_to("s3://bucket/dest.txt")
        == "aws s3 cp --no-progress s3://bucket/src.txt s3://bucket/dest.txt"
    )
    assert (
        File("s3://bucket/src.txt").shell_copy_to("dest.txt")
        == "aws s3 cp --no-progress s3://bucket/src.txt dest.txt"
    )
    assert (
        File("src.txt").shell_copy_to("s3://bucket/dest.txt")
        == "aws s3 cp --no-progress src.txt s3://bucket/dest.txt"
    )
    assert (
        File("s3://bucket/src.txt").shell_copy_to(None)
        == "aws s3 cp --no-progress s3://bucket/src.txt -"
    )
    assert (
        get_filesystem("s3").shell_copy(None, "s3://bucket/dest.txt")
        == "aws s3 cp --no-progress - s3://bucket/dest.txt"
    )

    assert (
        get_filesystem("s3").shell_copy("s3://bucket/src", "s3://bucket/dest", recursive=True)
        == "aws s3 cp --no-progress --recursive s3://bucket/src s3://bucket/dest"
    )
    with pytest.raises(ValueError):
        assert get_filesystem("s3").shell_copy("s3://bucket/src", None, recursive=True)


def test_shell_copy_gs() -> None:
    """
    GSFileSystem should be able to generate shell commands for file copies.
    """

    assert (
        File("gs://bucket/src.txt").shell_copy_to("gs://bucket/dest.txt")
        == "gsutil cp gs://bucket/src.txt gs://bucket/dest.txt"
    )
    assert (
        File("gs://bucket/src.txt").shell_copy_to("dest.txt")
        == "gsutil cp gs://bucket/src.txt dest.txt"
    )
    assert (
        File("src.txt").shell_copy_to("gs://bucket/dest.txt")
        == "gsutil cp src.txt gs://bucket/dest.txt"
    )
    assert File("gs://bucket/src.txt").shell_copy_to(None) == "gsutil cp gs://bucket/src.txt -"
    assert (
        get_filesystem("gs").shell_copy(None, "gs://bucket/dest.txt")
        == "gsutil cp - gs://bucket/dest.txt"
    )

    assert (
        get_filesystem("gs").shell_copy("gs://bucket/src", "gs://bucket/dest", recursive=True)
        == "gsutil cp -r gs://bucket/src gs://bucket/dest"
    )
    with pytest.raises(ValueError):
        assert get_filesystem("gs").shell_copy("gs://bucket/src", None, recursive=True)


def test_shell_copy_fs() -> None:
    """
    FileSystem should be able to generate shell commands for file copies.
    """
    # Cross cloud shell copies.
    assert (
        File("s3://bucket/src.txt").shell_copy_to("gs://bucket/dest.txt")
        == "(aws s3 cp --no-progress s3://bucket/src.txt -) | (gsutil cp - gs://bucket/dest.txt)"
    )
    assert (
        File("http://example.com/src.txt").shell_copy_to("gs://bucket/dest.txt")
        == "redun fs cp http://example.com/src.txt gs://bucket/dest.txt"
    )

    assert (
        Dir("s3://bucket/src").shell_copy_to("gs://bucket/dest")
        == "redun fs cp --recursive s3://bucket/src gs://bucket/dest"
    )


@use_tempdir
def test_copy_file() -> None:
    """
    Copy a file with copy_file().
    """
    src_file = File("src.txt")
    src_file.write("hello")
    dest_file = File("dest.txt")

    redun.file.copy_file(src_file.path, dest_file.path)
    assert dest_file.read() == src_file.read()


@use_tempdir
def test_copy_file_dir() -> None:
    """
    Copy a directory with copy_file().
    """
    a = File("src/a.txt")
    a.write("a")
    b = File("src/b/b.txt")
    b.write("b")

    dest_dir = Dir("dest")

    redun.file.copy_file("src", dest_dir.path, recursive=True)
    assert {file.path for file in dest_dir} == {"dest/a.txt", "dest/b/b.txt"}


def test_non_existent_file_error() -> None:
    """
    File.open should raise descriptive error when path does not exist.
    """

    with pytest.raises(
        FileNotFoundError, match=r"\[Errno 2\] No such file or directory\. not_exist_file"
    ):
        File("not_exist_file").open()


def test_file_error() -> None:
    """
    File.open should raise descriptive error when there is a low-level error.
    """

    def file_open(*args):
        raise OSError(100, "My OS error")

    fs = get_filesystem(proto="local")
    with patch.object(fs, "_open", file_open):
        with pytest.raises(OSError, match=r"\[Errno 100\] My OS error\. bad_file"):
            File("bad_file").open()


# Root user will still be able to write to a readonly file so skip this test if we are running
# as root (which will be the case on codebuild runs).
@use_tempdir
@pytest.mark.skipif(os.getuid() == 0, reason="Running as root which can write to readonly files")
def test_readonly_file_error() -> None:
    """
    File.open should raise descriptive error when trying to open a readonly file in write mode.
    """
    File("readonly_file").write("hi")
    os.system("chmod 400 readonly_file")
    with pytest.raises(PermissionError, match=r"\[Errno 13\] Permission denied\. readonly_file"):
        File("readonly_file").open("w")


@use_tempdir
def test_ifile() -> None:
    """
    IFile hash should only depend on path.
    """

    # First confirm that regular files update hash based on contents and timestamp.
    file1 = File("file")
    file1.write("1")

    file2 = File("file")
    file2.write("22")

    assert file1.get_hash() != file2.get_hash()

    # IFile hashes only depend on path.
    file1 = IFile("ifile")
    file1.write("1")

    file2 = IFile("ifile")
    file2.write("22")

    assert file1.get_hash() == file2.get_hash()


@use_tempdir
def test_ifile_classes() -> None:
    """
    IFile methods should return related objects of the right type.
    """
    assert isinstance(IFile("file").stage("local"), IStagingFile)
    assert isinstance(IDir("dir").file("file"), IFile)

    IFile("dir/a").touch()
    IFile("dir/b").touch()
    assert all(isinstance(file, IFile) for file in IDir("dir"))


@use_tempdir
def test_content_file() -> None:
    """
    ContentFile hash should only depend on path and content (not timestamp).
    """

    # Hash should only depend on path.
    file1 = ContentFile("file")
    file1.write("hi")

    file2 = ContentFile("file")
    file2.write("hi")

    assert file1.get_hash() == file2.get_hash()

    # Hash should update with content.
    file2.write("bye")
    assert file1.get_hash() != file2.get_hash()

    # Hash should go back after update.
    file2.write("hi")
    assert file1.get_hash() == file2.get_hash()

    # Different content should change the hash.
    file1 = ContentFile("file")
    file1.write("hi")

    file2 = ContentFile("file")
    file2.write("bye")

    assert file1.get_hash() != file2.get_hash()


@use_tempdir
def test_content_file_classes() -> None:
    """
    ContentFile methods should return related objects of the right type.
    """
    assert isinstance(ContentFile("file").stage("local"), ContentStagingFile)
    assert isinstance(ContentDir("dir").file("file"), ContentFile)

    ContentFile("dir/a").touch()
    ContentFile("dir/b").touch()
    assert all(isinstance(file, ContentFile) for file in ContentDir("dir"))
