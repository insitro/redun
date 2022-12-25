"""
Example of a map-reduce type operation where a list of A_i is mapped to a list of B_i
and then reduced to produce C.
The B_i are stored in intermediate values, and we wish to delete them after C has been computed.

In this example we use check_valid="shallow" so the deletion of B_i does not cause the
re-computation of C, but at the same time a change in any A_i will.


Run:
redun run map_reduce.py main
# re run to verify that all jobs were cached
redun run map_reduce.py main
# modify a value in the list ["A_1", "A_2", "A_3"]
# Verify that the computation is re-triggered
redun run map_reduce.py main

"""
from typing import List

from redun import File, functools, task

redun_namespace = "redun.examples.memoization_patterns"


@task()
def generate_a_values() -> List[str]:
    """
    Generates a list of strings.
    Changes in this list should re trigger computation of C
    """
    return ["A_1", "A_2", "A_3"]


@task()
def map_a_to_b(a_val: str) -> File:
    """
    Maps values "A_i" to "B_i" and stores the resulting string in a file.
    Deletion of these files should *not* re trigger the computation of C
    """
    # Create B_i from A_i
    b_val = f"B_{a_val[-1]}"

    # Create a file to store the resulting b
    file_path = f"./b_file_{b_val[-1]}"
    b_file = File(file_path)
    b_file.write(b_val)

    return b_file


@task()
def delete_b_files(intermediate_files: List[File]) -> None:
    """
    Deletes a list of files
    """
    for f in intermediate_files:
        f.remove()


@task()
def reduce_b_files_into_c(files: [List[File]]) -> str:
    """
    Loads all the files in a list and concatenates the first line of each
    """
    b_strings = [file.read() for file in files]

    # Concatenate
    c_val = " ".join(b_strings)

    return c_val


@task()
def use_c(c_val: str) -> str:
    """
    Prints a string
    """
    print(c_val)
    return c_val


@task(check_valid="shallow")
def map_reduce_no_caching(a_values: List[str]) -> str:

    # Maps the values of A to a list of file promises that will be evaluated later
    b_val_file_promises = [map_a_to_b(a) for a in a_values]
    # Promises to reduce the values from the files to generate a string c
    c = reduce_b_files_into_c(b_val_file_promises)
    # Indicates that c should be evaluated before delete_b_files. This way b_files are used
    # before deletion
    c_value = functools.seq((c, delete_b_files))[0]
    return c_value


@task()
def main() -> str:
    a_values = generate_a_values()
    c = map_reduce_no_caching(a_values)
    return use_c(c)
