"""
Example of a task that can parse an enum from the command line.

Try:
    $ redun run workflow.py main help

    optional arguments:
      -h, --help            show this help message and exit
      --color {Color.red,Color.green,Color.blue}
                            (default: Color.blue)

Also Try:
    $ redun run workflow.py main --color green

Or:
    $ redun run workflow.py main --color Color.green
"""

from enum import Enum

from redun import task


redun_namespace = "redun.examples.enum"


class Color(Enum):
    red = 1
    green = 2
    blue = 3


@task()
def main(color: Color=Color.blue):
    """
    Example of a task that can parse an enum from the command line.
    """
    return color
