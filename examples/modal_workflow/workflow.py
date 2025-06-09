from redun import task
import modal

redun_namespace = "redun.examples.modal_workflow"


@task()
def task3(x):
    print("task3")
    return [x["first"], x["second"]]


@task(
    executor="modal",
    modal_args={
        "cpu": 10,
        "mounts": [
            modal.Mount.from_local_dir(
                "./examples/01_hello_world", remote_path="/root/hello_world"
            )
        ],
        "network_file_systems": {
            "/mnt/nfs": modal.NetworkFileSystem.from_name("my-redun-nfs", create_if_missing=True)
        },
    },
)
def task2(x):
    print("task2")

    import os

    print(os.listdir("/root/hello_world"))

    with open("/mnt/nfs/test.txt", "w") as f:
        f.write("hello")

    print(os.listdir("/mnt/nfs"))

    return task3(x) + x["list"]


@task(
    executor="modal",
    modal_args={"cloud": "gcp", "secrets": [modal.Secret.from_name("my-huggingface-secret")]},
)
def task1(x):
    print("task1")
    return task2(x)


@task(
    executor="modal",
    modal_args={"region": "eu", "image": modal.Image.debian_slim().pip_install("redun", "pandas")},
)
def task4(x):
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    print(df.head())

    print("task4")
    return [x, x]


@task(executor="modal", script=True)
def ls(path: str):
    # Calls grep to find pattern in a file.

    import time

    time.sleep(1)
    return f"""
    ls {path}
    """


@task(
    executor="modal",
    modal_args={
        "gpu": "A10G",
        "volumes": {"/mnt/vol": modal.Volume.from_name("my-redun-volume", create_if_missing=True)},
    },
)
def main(x: str = "hi", y: str = "bye"):
    print("main")
    a = task4(x)
    b = task1({"list": [y], "first": a[0], "second": a[1]})

    with open("/mnt/vol/test.txt", "w") as f:
        f.write("hello")

    import os

    print(os.listdir("/mnt/vol"))


    return a + b + [ls("/root")]

