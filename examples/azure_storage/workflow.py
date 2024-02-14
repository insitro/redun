import pandas as pd

from redun import Dir, File, task

redun_namespace = "azure_fs_tests"


# common values
DEFAULT_CONTAINER = "sample-data"


@task()
def read_and_write_files(
    account: str, container: str = DEFAULT_CONTAINER, output_file_name: str = "iris.csv"
) -> File:
    path = f"az://{container}@{account}.blob.core.windows.net/inputs/iris.csv"
    output_path = (
        f"az://{container}@{account}.blob.core.windows.net/outputs/redun/" + output_file_name
    )
    redun_file = File(path)

    with redun_file.open("r") as f:
        df2 = pd.read_csv(f)

    print(df2.head())

    staged_file = redun_file.stage().stage()
    print(staged_file.path)

    out_file = File(output_path)
    redun_file.copy_to(out_file)

    return out_file


@task()
def copy_to_another_account(dest_account: str, input_file: File) -> File:
    """
    This task copies a file to the same path in a different storage account
    """
    from redun.file import AzureBlobFileSystem

    # This task copies a file to the same path in a different storage account
    output_path = input_file.path.replace(
        AzureBlobFileSystem.get_account_name_from_path(input_file.path), dest_account
    )

    input_file.copy_to(File(output_path))
    return File(output_path)


@task(cache=False)
def display_dir_and_file_info(dir_name: Dir) -> None:
    """
    Executing some read-only operations on Dir.
    """
    # Displays some info using fs methods
    print(dir_name.filesystem.glob(dir_name.path + "/**"))
    # now display as redun files - it calls glob internally
    print(dir_name.files())
    print(dir_name.exists())

    print(dir_name.filesystem.isdir(dir_name.path))
    print(dir_name.filesystem.isfile(dir_name.path))

    return


@task()
def create_and_copy_dir(
    account: str, dest_dir_name: str, container: str = DEFAULT_CONTAINER
) -> Dir:
    """
    This task creates a directory in the same azure container and
    copies the contents of the "inputs" dir in provided source container.
    """
    src_dir = Dir(f"az://{container}@{account}.blob.core.windows.net/inputs/")
    dest_dir = Dir(
        f"az://{container}@{account}.blob.core.windows.net/outputs/redun/{dest_dir_name}"
    )
    dest_dir.mkdir()
    src_dir.copy_to(dest_dir)
    dest_dir.update_hash()
    return dest_dir


@task()
def remove_files_and_dirs(account: str, container: str = DEFAULT_CONTAINER):
    """
    This task creates and removes some files in provided container.
    Assumes "inputs" dir in the container contains files to be used for this test.
    """
    src_dir = Dir(f"az://{container}@{account}.blob.core.windows.net/inputs/")

    # create and remove empty dir
    new_empty_dir = Dir(
        f"az://{container}@{account}.blob.core.windows.net/outputs/redun/removal_test_0"
    )

    # Check if the directory exists. For empty cloud directories,
    # it's acceptable that they are not present.
    new_empty_dir.mkdir()
    print(f"Empty dir test, exists: {new_empty_dir.exists()}")
    new_empty_dir.rmdir()
    print(f"Empty dir test after removal, exists: {new_empty_dir.exists()}")

    # remove whole dir
    new_dir1 = Dir(
        f"az://{container}@{account}.blob.core.windows.net/outputs/redun/removal_test_1"
    )
    src_dir.copy_to(new_dir1)
    print(f"Non-empty dir: {new_dir1.exists()}")
    new_dir1.rmdir(recursive=True)
    print(f"Non-empty dir after removal: {new_dir1.exists()}")

    # remove files, then remove dir
    new_dir2 = Dir(
        f"az://{container}@{account}.blob.core.windows.net/outputs/redun/removal_test_2"
    )
    src_dir.copy_to(new_dir2)
    print(f"Non-empty dir 2: {new_dir2.exists()}, content: {new_dir2.files()}")
    try:
        new_dir2.rmdir()
    except OSError:
        print("cannot remove non-empty dir, correct behavior")

    new_file = new_dir2.file("iris.csv")
    new_file.remove()
    ex = new_file.exists()
    print(
        f"Non-empty dir 2 after removing content: {new_dir2.files()}, "
        f"file exists: {ex}, dir exists: {new_dir2.exists()}"
    )

    new_dir2.rmdir()
    print(f"Non-empty dir 2 after removal: {new_dir2.exists()}")
