import logging
import re
from itertools import chain
from typing import Any, Dict, List, Optional

from redun.backends.db import Argument, Execution, Job, Tag, Task, Value
from redun.tags import format_tag_value
from redun.utils import trim_string

NULL = object()
logger = logging.getLogger("redun.console")
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler("/tmp/redun.log"))


def log_write(*args: Any) -> None:
    """
    Debugging function to use when developing with Textual.
    """
    logger.info(" ".join(map(str, args)))


def format_link(link_pattern: str, tags: Dict[str, Any]) -> Optional[str]:
    """
    Format a link pattern using a tag dictionary.
    """

    def replace(match: re.Match) -> str:
        # Trim {{ }} brackets.
        key_regex = match[0][2:-2]

        # Parse out key and optional regex.
        if ":" in key_regex:
            key, regex = key_regex.split(":", 1)
        else:
            key, regex = key_regex, ""

        # Fetch value
        value = tags.get(key, NULL)
        if value is NULL:
            raise KeyError(key)
        value = str(value)

        if regex:
            # If a regex is given, reformat the value according to the regex.
            match2 = re.match(regex, value)
            if match2:
                try:
                    # Try a named group first.
                    value = match2["val"]
                except IndexError:
                    value = match2[1]
            else:
                raise KeyError(key)
        return value

    try:
        # Replace every instance of '{{varible}}' and '{{variable:regex}}'.
        return re.sub(r"\{\{([^}]|\}[^}])+\}\}", replace, link_pattern)
    except KeyError:
        return None


def get_links(link_patterns: List[str], tags: List[Tag]) -> List[str]:
    """
    Get links from a list of link patterns and redun tags.
    """
    tags_dict: Dict[str, Any] = {tag.key: tag.value for tag in tags}
    return list(
        filter(None, [format_link(link_pattern, tags_dict) for link_pattern in link_patterns])
    )


def format_tags(tags: List[Tag], max_length: int = 100, color="#9999cc") -> str:
    """
    Format a set of tags.
    """
    if not tags:
        return ""

    def format_tag_key_value(key: str, value: Any) -> str:
        key = trim_string(key, max_length=max_length)
        value_str = trim_string(format_tag_value(value), max_length=max_length)
        return f"[{color}]{key}={value_str}[/]"

    tags = sorted(tags, key=lambda tag: tag.key)

    return ", ".join(format_tag_key_value(tag.key, tag.value) for tag in tags)


def format_arguments(args: List[Argument]) -> str:
    """
    Display CallNode arguments.

    For example, if `args` has 2 positional and 1 keyword argument, we would
    display that as:

        'prog', 10, extra_file=File(path=prog.c, hash=763bc10f)
    """
    pos_args = sorted(
        [arg for arg in args if arg.arg_position is not None], key=lambda arg: arg.arg_position
    )
    kw_args = sorted([arg for arg in args if arg.arg_key is not None], key=lambda arg: arg.arg_key)

    text = ", ".join(
        chain(
            (trim_string(repr(arg.value.preview)) for arg in pos_args),
            ("{}={}".format(arg.arg_key, trim_string(repr(arg.value.preview))) for arg in kw_args),
        )
    )
    return text


def format_job(job: Job) -> str:
    """
    Format a redun Job into a string representation.
    """
    args = format_arguments(job.call_node.arguments) if job.call_hash else ""
    return f"[bold]{job.task.fullname}[/][#999999]({args})[/]"


def format_traceback(job: Job) -> str:
    """
    Format the call stack from Execution down to the given Job.
    """

    # Determine job stack.
    job_stack = []
    current_job = job
    while current_job:
        job_stack.append(current_job)
        current_job = current_job.parent_job

    parts = ["[@click=screen.click_exec]Exec {exec}[/] > ".format(exec=job.execution.id[:8])]

    if len(job_stack) > 2:
        parts.append(
            "({num_jobs} {unit}) > ".format(
                num_jobs=len(job_stack) - 2,
                unit="Jobs" if len(job_stack) - 2 > 1 else "Job",
            )
        )
    if len(job_stack) > 1:
        parts.append(
            "[@click=screen.click_parent_job]Job {job_id} {task_name}[/] > ".format(
                job_id=job_stack[1].id[:8],
                task_name=job_stack[1].task.name,
            )
        )
    parts.append(
        "[bold]Job {job_id} {task_name}[/]".format(
            job_id=job_stack[0].id[:8],
            task_name=job_stack[0].task.name,
        )
    )
    if job.child_jobs:
        parts.append(f" > [@click=screen.children]{len(job.child_jobs)} child jobs[/]")

    return f"[bold]Traceback:[/b] {''.join(parts)}"


def style_status(status: str) -> str:
    """
    Returns styled text for a job/execution status.
    """
    status2style = {
        "DONE": "#55aa55",
        "FAILED": "#aa5555",
        "CACHED": "#5555aa",
    }
    if status in status2style:
        return f"[white on {status2style[status]}]{status.center(6)}[/]"
    else:
        return f"[{status.center(6)}]"


def format_record(record: Any) -> str:
    """
    Format a redun repo record (e.g. Execution, Job, etc) into a string.
    """
    if isinstance(record, Execution):
        return (
            f"Exec {record.id[:8]} {style_status(record.status)} "
            f"{record.job.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            f"[[bold]{record.job.task.namespace or 'no namespace'}[/bold]] "
        )
    elif isinstance(record, Job):
        return (
            f"Job {record.id[:8]} {style_status(record.status)} "
            f"{record.start_time.strftime('%Y-%m-%d %H:%M:%S')} "
        ) + format_job(record)
    elif isinstance(record, Task):
        return f"Task {record.hash[:8]} {record.fullname}"
    elif isinstance(record, Value):
        return f"Value {record.value_hash[:8]} {trim_string(repr(record.preview))}"
    else:
        return repr(record)
