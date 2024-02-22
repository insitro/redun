from typing import Optional

from workflow import main as main1

from redun import task

redun_namespace = "redun.examples.context2"


@task
def main(size: int = 10, flag: Optional[bool] = None) -> dict:

    if flag is not None:
        context = {"my_tool": {"flag": flag, "extra_arg": 1}}
    else:
        context = {}

    # We can override the the context of another task at runtime.
    return main1.update_context(context)(size)
