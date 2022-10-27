import os
from typing import Dict, List

from redun import File, task

redun_namespace = "redun.examples.compile"


@task()
def compile(c_file: File) -> File:
    """
    Compile one C file into an object file.
    """
    os.system("gcc -c {c_file}".format(c_file=c_file.path))
    return File(c_file.path.replace(".c", ".o"))


@task()
def link(prog_path: str, o_files: List[File]) -> File:
    """
    Link several object files together into one program.
    """
    os.system(
        "gcc -o {prog_path} {o_files}".format(
            prog_path=prog_path,
            o_files=" ".join(o_file.path for o_file in o_files),
        )
    )
    return File(prog_path)


@task()
def make_prog(prog_path: str, c_files: List[File]) -> File:
    """
    Compile one program from its source C files.
    """
    o_files = [compile(c_file) for c_file in c_files]
    prog_file = link(prog_path, o_files)
    return prog_file


# Definition of programs and their source C files.
files = {
    "prog": [
        File("prog.c"),
        File("lib.c"),
    ],
    "prog2": [
        File("prog2.c"),
        File("lib.c"),
    ],
}


@task()
def make(files: Dict[str, List[File]] = files) -> List[File]:
    """
    Top-level task for compiling all the programs in the project.
    """
    progs = [make_prog(prog_path, c_files) for prog_path, c_files in files.items()]
    return progs
