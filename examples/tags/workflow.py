from redun import File, apply_tags, task

redun_namespace = "redun.examples.tags"


# Tasks can be tagged using the `tags` task option.
@task(tags=[("step", "preprocess")])
def reverse_file(in_file: File, out_path: str) -> File:
    out_file = File(out_path)
    with out_file.open("w") as out:
        lines = list(reversed(list(in_file.open())))
        for line in lines:
            out.write(line)
    return {
        "file": apply_tags(out_file, [("output", "file")]),
        "lines": len(lines),
    }


@task()
def main(file: File = File("data.txt")) -> File:
    out_path = f"{file.path}.reversed"

    # Tags can be applied to a value.
    # apply_tags() returns the value back, although as an expression.
    file = apply_tags(file, [("input", "file")])

    return reverse_file(file, out_path)
