import os
from typing import Dict, List

from redun import File, script, task


@task()
def parse_int(text: str) -> int:
    """
    Small postprocessing task to parse a string to an int.
    """
    return int(text.strip())


@task()
def find_colors(data: File, column: int) -> List[str]:
    """
    Determine all the colors in a dataset.
    """
    colors = set()

    # Here, we show how to open a File to get a normal file stream.
    with data.open() as infile:
        for line in infile:
            row = line.strip().split("\t")
            colors.add(row[column])
    return list(colors)


@task()
def count_color(data: File, color: str) -> int:
    """
    Use a script to count people who like a particular color.
    """

    # script() will return the standard out of a shell script.
    # We use the f-string syntax to fill in parameters.
    # Feel free to indent the script to match the code,
    # dedenting is applied automatically.
    output = script(
        f"""
        awk '$3 == "{color}"' {data.path} | wc -l
        """
    )
    return parse_int(output)


@task()
def count_colors(data: File, colors: List[str]) -> Dict[str, int]:
    """
    Count how popular each color is.
    """
    # As we have seen previously, the color counting will be done in parallel.
    return {color: count_color(data, color) for color in colors}


@task()
def count_colors_by_script(data: File, output_path: str) -> Dict[str, File]:
    """
    Count colors using a multi-line script.
    """
    # This example task uses a multi-line script.
    # We also show how to isolate the script from other scripts that might be running.
    # By using `tempdir=True`, we will run this script within a temporary directory.

    # Use absolute paths for input and output because script will run in a new temp directory.
    data = File(os.path.abspath(data.path))
    output = File(os.path.abspath(output_path))
    log_file = File(os.path.abspath(output_path) + ".log")

    # Staging Files.
    # The `File.stage()` method pairs a local path (relative to current working directory
    # in the script) with a remote path (url or project-level path). This pairing is used
    # to automatically copy files to and from the temporary directory.
    #
    # staged_file: StagingFile = File(remote_path).stage(local_path)

    return script(
        """
        echo 'sorting colors...' >> log.txt
        cut -f3 data | sort > colors.sorted

        echo 'counting colors...' >> log.txt
        uniq -c colors.sorted | sort -nr > color-counts.txt
        """,
        # Use a temporary directory for running the script.
        tempdir=True,
        # Stage the input file to a local file called `data`.
        inputs=[data.stage("data")],
        # Unstage the output files to our project directory.
        # Final return value of script() takes the shape of outputs, but with each StagingFile
        # replaced by `File(remote_path)`.
        outputs={
            "colors-counts": output.stage("color-counts.txt"),
            "log": log_file.stage("log.txt"),
        },
    )


@task()
def main(data: File = File("data.tsv")) -> Dict[str, int]:
    """
    Perform some small data analysis by comparing two different techniques.
    """
    # Method 1.
    colors = find_colors(data, 2)
    color_counts = count_colors(data, colors)

    # Method 2.
    output_path = os.path.abspath("color-counts.txt")
    color_counts2 = count_colors_by_script(data, output_path)

    return {
        "method1": color_counts,
        "method2": color_counts2,
    }
