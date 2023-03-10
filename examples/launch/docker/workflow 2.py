from time import sleep

import redun

redun_namespace = "redun.examples.remote_run"


@redun.task()
def add(x: int, y: int) -> str:
    print(f"Adding up {x} and {y} (sleep 10)")
    sleep(10)
    return f"{x} + {y} = {x + y}"


@redun.task()
def saveoutput(filename: str, data: str) -> redun.File:
    output_file = redun.File(filename)
    with output_file.open("w") as fp:
        fp.write(data)
    return output_file


@redun.task()
def main(x: int, y: int, output: str = "output.txt") -> redun.File:
    result = add(x, y)
    return saveoutput(output, result)
