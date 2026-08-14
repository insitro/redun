import time

from redun import task
from redun.scheduler import with_resource


@task
def do_s3_copy(src_url: str, dest_url: str):
    # Pretend to copy a big file.
    print(f"copy {src_url} {dest_url}")
    time.sleep(2)
    return dest_url


@task
def main(n: int = 30):
    # Try to copy a lot of files at once.
    results = [
        with_resource(
            do_s3_copy(f"s3://a/{i}", f"s3://b/{i}"),
            resource_name="s3",
            retry_interval=3,  # Choose a fast retry interval for example sake.
            retry_giveup=600,  # Give up after 10 minutes of trying for example sake.
        )
        for i in range(n)
    ]
    return results
