import json
from code import InteractiveInterpreter
from collections import defaultdict
from typing import Any, Dict, List, Optional, cast

from rich.console import RenderableType
from rich.style import Style
from rich.text import Text
from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Input, Static

from redun.backends.db import Execution, Job, Tag, Task, Value
from redun.cli import format_timedelta
from redun.console.utils import format_job, get_links, style_status
from redun.scheduler import Job as SchedulerJob

NULL = object()


class RedunHeader(Static):
    """
    Header used on all Screen to display the current CLI command (e.g. argv).
    """

    CSS_PATH = "style.css"

    def __init__(self, title: str, **kwargs):
        super().__init__("[bold]redun console[/bold] " + title, **kwargs)

    def update(self, title: str) -> None:
        super().update("[bold]redun console[/bold] " + title)


class RedunFooter(Footer):
    """
    Footer used on Screens that use pagination.
    """

    page = reactive(1)

    def __init__(self, page: int):
        super().__init__()
        self.page = page

    def render(self) -> RenderableType:
        text = self._make_key_text()
        text.append_text(
            Text(f" Page {self.page} ", justify="right", style=Style(bgcolor="green"))
        )
        return text


class Table(DataTable):
    """
    Generic DataTable that uses Enter key to select cells.
    """

    class Selected(Message):
        def __init__(self, table: "Table"):
            super().__init__()
            self.table = table

    def key_enter(self, event: events.Key) -> None:
        """
        Callback for pressing enter key.
        """
        return self.post_message(self.Selected(self))


class ExecutionList(DataTable):
    """
    Table of redun Executions.
    """

    executions = reactive([])

    class Selected(Message):
        def __init__(self, execution: Execution):
            super().__init__()
            self.execution = execution

    def __init__(self, executions: Optional[List[Execution]], **kwargs):
        super().__init__(**kwargs)
        self.cursor_type = "row"
        self.add_columns("ID", "Status", "Started", "Duration", "Execution")
        self.executions = executions

    def watch_executions(self) -> None:
        """
        Set the list of Executions to display.
        """
        self.clear()

        if self.executions is None:
            self.add_row("Loading...")
            return

        for execution in self.executions:
            self.add_row(
                f"Exec {execution.id[:8]}",
                style_status(execution.status),
                execution.job.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                format_timedelta(execution.job.duration) if execution.job.duration else "",
                f"[[bold]{execution.job.task.namespace or 'no namespace'}[/bold]] "
                + " ".join(json.loads(execution.args)[1:]),
            )

    def key_enter(self, event: events.Key) -> None:
        """
        Callback for pressing enter key on an Execution.
        """
        if isinstance(self.executions, list):
            execution = self.executions[self.cursor_coordinate.row]
            return self.post_message(self.Selected(execution))


class JobList(DataTable):
    """
    Table of redun Jobs.
    """

    jobs = reactive([])

    class Selected(Message):
        def __init__(self, job: Job):
            super().__init__()
            self.job = job

    def __init__(self, jobs: Optional[List[Job]], **kwargs):
        super().__init__(**kwargs)
        self.cursor_type = "row"

        self.add_columns("ID", "Status", "Started", "Duration", "Job")
        self.jobs = jobs

    def watch_jobs(self) -> None:
        # Clear table.
        self.clear()

        if self.jobs is None:
            self.add_row("Loading...")
            return

        for job in self.jobs:
            self.add_row(
                f"{' ' * min(job.depth, 10)}Job {job.id[:8]}",
                style_status(job.status),
                job.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                format_timedelta(job.duration) if job.duration else "",
                format_job(job),
            )

    def key_enter(self, event: events.Key) -> None:
        """
        Callback for pressing enter key on a Job.
        """
        job = self.jobs[self.cursor_coordinate.row]
        return self.post_message(self.Selected(job))


class JobStatusTable(Table):
    """
    Table of redun Job statuses by task.
    """

    def __init__(self, execution_id: str, **kwargs):
        super().__init__(**kwargs)
        self.execution_id = execution_id
        self.add_columns("TASK", *SchedulerJob.STATUSES)
        self.add_row("Loading...")

        self.tasks: List[Optional[Task]] = []

    def on_mount(self) -> None:
        self.call_after_refresh(self.load_jobs)

    async def load_jobs(self) -> None:
        # Clear loading message.
        self.clear()

        # Fetch jobs and task names from db.
        self.job_tasks = (
            self.app.session.query(Job, Task.namespace, Task.name)
            .join(Task, Job.task_hash == Task.hash)
            .filter(Job.execution_id == self.execution_id)
            .all()
        )

        tasks = (
            self.app.session.query(Task)
            .join(Job, Job.task_hash == Task.hash)
            .filter(Job.execution_id == self.execution_id)
            .all()
        )
        name2task = {task.fullname: task for task in tasks}

        # Compute status counts.
        job_status_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for job, namespace, name in self.job_tasks:
            fullname = namespace + "." + name if namespace else name
            job_status_counts[fullname][job.status] += 1
            job_status_counts[fullname]["TOTAL"] += 1
            job_status_counts["ALL"][job.status] += 1
            job_status_counts["ALL"]["TOTAL"] += 1

        # Populate table.
        task_names = sorted(job_status_counts.keys())
        for task_name in task_names:
            self.add_row(
                task_name,
                *[str(job_status_counts[task_name][status]) for status in SchedulerJob.STATUSES],
            )

        self.tasks = [name2task.get(task_name) for task_name in task_names]


class ValueSpan(Static):
    """
    Rendering of a redun Value that is linkable and collapsible.
    """

    expanded = reactive(False)

    def __init__(
        self,
        value: Value,
        format: str = "{value}",
        max_size: int = 100,
        on_click=lambda value_hash: None,
    ):
        super().__init__()
        self.value_hash = value.value_hash

        self.value_long = repr(value.preview)
        self.value_short = self.value_long[: max_size - len(format) + len("{value}")]

        self.format = format.format(value="[@click=click]{value}[/]")
        self.on_click = on_click

    def action_click(self) -> None:
        self.on_click(self.value_hash)

    def action_expand(self) -> None:
        self.expanded = not self.expanded
        self.refresh(layout=True)

    def render(self) -> Any:
        if self.value_short != self.value_long:
            if self.expanded:
                return self.format.format(value=self.value_long) + " [@click=expand](less)[/]"
            else:
                return self.format.format(value=self.value_short) + "[@click=expand]...[/]"
        else:
            return self.format.format(value=self.value_long)


class Interpreter(InteractiveInterpreter):
    """
    Interactive Python interpreter used by the ReplScreen.
    """

    def __init__(self, locals, write):
        super().__init__(locals)

        # Set output callback.
        self.write = write

    def run(self, code: str) -> Any:
        """
        Execute a code object.
        """
        try:
            try:
                # Try to evaluate as an expression first and return its result.
                return eval(code, cast(dict, self.locals))
            except SyntaxError:
                # If syntax error, it might be due to assignments being used.
                # Fallback to exec() instead.
                exec(code, cast(dict, self.locals))
                return NULL
        except SystemExit:
            raise
        except Exception:
            self.showtraceback()
            raise


class CommandInput(Input):
    """
    Input widget with history and tab-completion.
    """

    class Complete(Message):
        def __init__(self, text: str):
            super().__init__()
            self.text = text

    BINDINGS = [
        Binding("up", "cursor_up", "cursor up", show=False),
        Binding("down", "cursor_down", "cursor down", show=False),
    ]

    def __init__(self, *args, complete: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        if complete:
            self._bindings.bind("tab", "complete", "Complete", show=False)
        self.history: List[str] = []
        self.position = -1

    def on_input_submitted(self, message: Input.Submitted) -> None:
        # Add command to history.
        self.history.append(message.value)
        self.position = len(self.history)

    def action_cursor_up(self) -> None:
        if self.position > 0:
            # Move backwards through command history.
            self.position -= 1
            self.value = self.history[self.position]

    def action_cursor_down(self) -> None:
        if self.position < len(self.history):
            # Move forward in command history.
            self.position += 1
            if self.position < len(self.history):
                self.value = self.history[self.position]
            else:
                # When advancing off the end of history, give a blank command.
                self.value = ""

    def action_complete(self) -> None:
        """
        Callback for tab-complete attempt.
        """
        self.post_message(self.Complete(self.value))


class TagLinks(Container):
    """
    Displays links derived from redun Tags.
    """

    def __init__(self, link_patterns: List[str], tags: List[Tag]):
        super().__init__(classes="links")
        self.links = get_links(link_patterns, tags)

    def compose(self) -> ComposeResult:
        if self.links:
            yield Static("[bold]Links:[/]")
            for link in self.links:
                yield Static(f"  [#9999cc]{link}[/]")
