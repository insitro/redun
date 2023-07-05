import argparse
import json
import shlex
from collections import defaultdict
from pprint import pformat
from typing import Any, Dict, Iterator, List, Optional

import sqlalchemy as sa
from fancycompleter import Completer
from rich.syntax import Syntax
from sqlalchemy.orm import joinedload
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.message import Message
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Footer, Input, Label, Static, TextLog

from redun import File
from redun.backends.db import Argument, CallNode, Execution
from redun.backends.db import File as DbFile
from redun.backends.db import Job, Subvalue, Tag, Task, Value
from redun.backends.db.query import CallGraphQuery, parse_callgraph_query, setup_query_parser
from redun.cli import format_timedelta
from redun.console.parser import format_args, parse_args
from redun.console.utils import format_record, format_tags, format_traceback, style_status
from redun.console.widgets import (
    CommandInput,
    ExecutionList,
    Interpreter,
    JobList,
    JobStatusTable,
    RedunFooter,
    RedunHeader,
    Table,
    TagLinks,
    ValueSpan,
)
from redun.scheduler import ErrorValue
from redun.scheduler import Job as SchedulerJob
from redun.task import split_task_fullname
from redun.utils import trim_string

NULL = object()
DEFAULT_PAGE_SIZE = 100


class RedunScreen(Screen):
    """
    Base Textual Screen for redun.
    """

    path_pattern = ""

    def __init__(self):
        super().__init__()
        self.parser = self.get_parser()

        # Ideally self.args should be reactive, but since it's a nested object
        # we manage the watch calls ourselves.
        self.args = parse_args(self.parser)
        self.args_prev = None

        self.header = RedunHeader(self.get_path(), id="title")

    def get_path(self) -> str:
        """
        Returns the path element of the argv (the first argument).
        """
        raise NotImplementedError("path not defined")

    @classmethod
    def get_parser(self) -> argparse.ArgumentParser:
        return argparse.ArgumentParser(prog="", exit_on_error=False)

    def get_options(self) -> str:
        """
        Returns the options as single string.
        """
        return shlex.join(format_args(self.parser, self.args))

    def parse(self, argv: List[str]) -> None:
        """
        Parse command-line arguments and update screen.
        """
        self.args = parse_args(self.parser, argv)
        self.notify_args()

    def notify_args(self) -> None:
        """
        Notify the screen that self.args has changed.
        """
        if self.args.__dict__ != self.args_prev:
            # Only when self.args changes do we call self.watch_args().
            self.args_prev = dict(self.args.__dict__)
            self.watch_args()

    def watch_args(self) -> None:
        """
        Update screen with new arguments.
        """
        self.header.update(self.get_path() + " " + self.get_options())

    def action_back(self) -> None:
        """
        Callback for popping the screen.
        """
        self.app.close_screen()

    def action_filter(self) -> None:
        """
        Callback for displaying the filter screen for the current screen.
        """
        screen = self.app.goto_screen(FilterScreen, self.__class__.__name__ + "-filter", (self,))
        screen.input.value = self.get_options()

    def on_mount(self):
        self.notify_args()

    def compose(self) -> ComposeResult:
        yield Container(
            self.header,
            Footer(),
        )


class FilterScreen(Screen):
    """
    Generic Screen for editing the parent Screen filters.
    """

    CSS_PATH = "style.css"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
    ]

    def __init__(self, parent_screen: RedunScreen):
        super().__init__()
        self.input = CommandInput(placeholder="Enter filters...", complete=False)
        self.parent_screen = parent_screen
        self.help = parent_screen.parser.format_help()

    def on_input_submitted(self, message):
        self.app.pop_screen()
        self.parent_screen.parse([self.parent_screen.get_path()] + shlex.split(message.value))

    def compose(self) -> ComposeResult:
        self.input.focus()

        yield Container(
            Label("Filter:"),
            self.input,
            Static(self.help, classes="filter-help"),
            classes="filter",
        )


class HelpScreen(Screen):
    """
    Modal Help Screen that is launched from a parent Screen.
    """

    CSS_PATH = "style.css"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
    ]

    def __init__(self, parent_screen: RedunScreen):
        super().__init__()
        self.parent_screen = parent_screen
        self.help = parent_screen.parser.format_help()

    def compose(self) -> ComposeResult:

        yield Container(
            Label("Help:"),
            Static(self.help, classes="help-text"),
            classes="help-dialog",
        )


class ReplScreen(RedunScreen):
    """
    Screen for read-eval-print-loop (REPL).
    """

    path_pattern = r"repl(/(.*))?"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
        ("c", "clear", "Clear"),
    ]

    def __init__(self, locals={}, obj_id: Optional[str] = None):
        self.obj_id = obj_id
        super().__init__()
        self.text_log = TextLog()
        self.input = CommandInput(placeholder="Execute Python code...")

        def query(expr: Any, *args: Any, **kwargs: Any) -> Any:
            """
            Query the redun repo for records.
            """
            if isinstance(expr, str):
                return CallGraphQuery(self.app.session).like_id(expr).first()
            else:
                return self.app.session.query(expr, *args, **kwargs)

        def console(obj: Any) -> None:
            """
            Go to the redun console screen for an object or id.
            """
            if isinstance(obj, str):
                self.app.route([obj])
            elif isinstance(obj, Execution):
                self.app.route([obj.id])
            elif isinstance(obj, Job):
                self.app.route([obj.id])
            elif isinstance(obj, Task):
                self.app.route([obj.hash])
            elif isinstance(obj, Value):
                self.app.route([obj.value_hash])

        # Add extra built-in functions for the repl.
        locals.update(
            {
                "sa": sa,
                "session": self.app.session,
                "query": query,
                "console": console,
                "File": File,
                "Argument": Argument,
                "CallNode": CallNode,
                "Execution": Execution,
                "Job": Job,
                "Tag": Tag,
                "Task": Task,
                "Value": Value,
                "Subvalue": Subvalue,
                "DbFile": DbFile,
            }
        )
        self.interpreter = Interpreter(locals, self.on_write)

        self.update({}, obj_id)
        self.write_intro()

    def write_intro(self) -> None:
        """
        Write intro text to log.
        """
        self.text_log.write("# This is a Python Read-Eval-Print-Loop (REPL).")
        self.text_log.write("# Several helpful functions are provided:")
        self.text_log.write(
            "#   `query(id)` can be used to query any redun record by it's id or hash "
        )
        self.text_log.write(
            "#   `query(Model)` can be used to query using a sqlalchemy model, "
            "such as Execution, Job, Task, Value, etc."
        )
        self.text_log.write(
            "#   `console(model)` can be used to go to the console screen for a specific "
            "record (Execution, Job, Task, Value, etc)."
        )
        self.text_log.write("")

    def get_path(self) -> str:
        if self.obj_id:
            return f"repl/{self.obj_id}"
        else:
            return "repl"

    def action_clear(self) -> None:
        """
        Clear repl output.
        """
        self.text_log.clear()

    def update(self, locals={}, obj_id: Optional[str] = None) -> None:
        """
        Update the repl environment with new values.
        """
        self.obj_id = obj_id
        if obj_id:
            self.obj = CallGraphQuery(self.app.session).like_id(obj_id).first()
        else:
            self.obj = None
        if self.obj is not None:
            locals["obj"] = self.obj

        self.interpreter.locals.update(locals)  # type: ignore

        for key in locals:
            self.text_log.write(f"{key} = {trim_string(repr(locals[key]))}")
        self.text_log.write("")

    def on_input_submitted(self, message) -> None:
        """
        Callback for when input command is submitted.
        """
        self.text_log.write(f"> {message.value}")
        try:
            result = self.interpreter.run(message.value)
            if result is not NULL:
                self.text_log.write(result)
        except Exception:
            pass
        self.input.value = ""

    def on_command_input_complete(self) -> None:
        """
        Callback for tab-complete action.
        """
        env = self.interpreter.run("globals()")
        completer = Completer(env)
        completion = completer.complete(self.input.value, 0)
        if completion is not None and completion.startswith(self.input.value):
            self.input.value = completion
            self.input.cursor_position = len(completion)

    def on_write(self, data: str) -> None:
        """
        Callback for when the interpreter wants to write to the output log.
        """
        self.text_log.write(data)

    def on_mount(self) -> None:
        super().on_mount()
        self.input.focus()

    def compose(self) -> ComposeResult:
        yield Container(
            self.header,
            Container(
                self.text_log,
                self.input,
                id="repl-screen",
            ),
            Footer(),
        )


class SearchScreen(RedunScreen):
    """
    Screen for doing general record searching.
    """

    path_pattern = "^search$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
        ("n", "next", "Next page"),
        ("p", "prev", "Previous page"),
        ("/", "filter", "Filter"),
        ("r", "repl", "REPL"),
    ]

    results = reactive(None)
    page = reactive(1)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.page_size = DEFAULT_PAGE_SIZE

        self.input = CommandInput("", complete=False, placeholder="Enter search...")
        self.results_table = Table(id="search-results")
        self.results_table.add_columns("Search results")

    def get_path(self) -> str:
        return "search"

    @classmethod
    def get_parser(self) -> argparse.ArgumentParser:
        return setup_query_parser(argparse.ArgumentParser(prog="", exit_on_error=False))

    def action_next(self) -> None:
        """
        Callback for next page of results.
        """
        self.page += 1
        self.query_one(RedunFooter).page = self.page

    def action_prev(self) -> None:
        """
        Callback for previous page of results.
        """
        if self.page > 1:
            self.page -= 1
            self.query_one(RedunFooter).page = self.page

    def watch_page(self) -> None:
        self.call_after_refresh(self.load_results)

    def watch_args(self):
        super().watch_args()
        self.input.value = self.get_options()
        self.call_after_refresh(self.load_results)

    def watch_results(self) -> None:
        self.results_table.clear()

        if self.results is None:
            self.results_table.add_row("Loading...")
            return

        for record in self.results:
            self.results_table.add_row(format_record(record))

    def on_input_submitted(self, message: Input.Submitted) -> None:
        """
        Callback for when input command is submitted.
        """
        argv = shlex.split(self.input.value)
        self.args = parse_args(self.parser, argv)
        self.input.value = ""
        self.watch_args()

    def on_table_selected(self, message: Table.Selected) -> None:
        """
        Callback for pressing enter key on a result.
        """
        if self.results:
            record = self.results[self.results_table.cursor_coordinate.row]
        self.app.goto_record(record)

    async def load_results(self) -> None:

        if format_args(self.parser, self.args) == []:
            self.results = []
            return

        query = CallGraphQuery(self.app.session)
        query = parse_callgraph_query(query, self.args)
        query = query.build()

        # Prefetch joined data.
        query = query.clone(
            jobs=query._jobs.options(
                joinedload(Job.task),
                joinedload(Job.call_node)
                .joinedload(CallNode.arguments)
                .joinedload(Argument.value),
            )
        )

        self.results = list(query.page(self.page - 1, self.page_size))

    def compose(self) -> ComposeResult:
        self.results_table.focus()
        yield Container(
            self.header,
            self.input,
            Container(
                self.results_table,
                id="search-screen",
            ),
            RedunFooter(page=self.page),
        )


class ValueScreen(RedunScreen):
    """
    Screen for exploring a redun Value.
    """

    path_pattern = "^values/(.*)$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "back", "Back"),
        ("r", "repl", "REPL"),
    ]

    def __init__(self, value_hash: str):
        self.value_hash = value_hash
        super().__init__()

        self.value = self.app.session.get(Value, value_hash)

    def get_path(self) -> str:
        return f"values/{self.value_hash}"

    def action_upstream_jobs(self) -> None:
        self.app.route(["search", "--result", self.value_hash])

    def action_downstream_jobs(self) -> None:
        self.app.route(["search", "--arg", self.value_hash])

    def action_click_value(self, value_hash: str) -> None:
        self.app.route([f"values/{value_hash}"])

    def action_upstream(self) -> None:
        self.app.route([f"upstreams/{self.value_hash}"])

    def action_repl(self) -> None:
        screen = self.app.get_screen("ReplScreen")
        screen.update(
            {"value": self.value.value_parsed or self.value.preview}, obj_id=self.value_hash
        )
        self.app.push_screen(screen)

    def compose(self) -> ComposeResult:
        subvalues = Container(id="value-subvalues")
        if self.value is None:
            body = Static("Unknown value")
            upstreams = ""
            downstreams = ""
            tags = []
        else:
            body = Static(
                Syntax(
                    code=pformat(self.value.value_parsed or self.value.preview),
                    lexer="Python",
                    line_numbers=False,
                    word_wrap=True,
                ),
            )
            # Determine subvalues.
            if self.value.children:
                subvalue_rows = [
                    Static(
                        f"  - [@click=screen.click_value('{subvalue.value_hash}')]{format_record(subvalue)}[/]"  # noqa: E501
                    )
                    for subvalue in self.value.children
                ]
                subvalues = Container(
                    Static("[bold]Subvalues[/]"),
                    *subvalue_rows,
                    id="value-subvalues",
                )

            query = CallGraphQuery(self.app.session)
            upstreams = f"({query.filter_results([self.value_hash])._jobs.count()})"
            downstreams = f"({query.filter_arguments([self.value_hash])._jobs.count()})"
            tags = self.value.tags

        yield Container(
            self.header,
            Container(
                Static(f"[bold]Value[/] {self.value_hash}"),
                Static("[bold]Tags:[/] " + format_tags(tags)),
                TagLinks(self.app.link_patterns, tags),
                Static(),
                Static(f"  [@click=screen.upstream_jobs]Upstream jobs {upstreams}[/]"),
                Static(f"  [@click=screen.downstream_jobs]Downstream jobs {downstreams}[/]"),
                Static("  [@click=screen.upstream]Upstream dataflow[/]"),
                Static(),
                subvalues,
                Static(),
                body,
                id="value-screen",
            ),
            Footer(),
        )


class FilesScreen(RedunScreen):
    """
    Top-level Screen for exploring all redun Files.
    """

    path_pattern = r"^files$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
        ("n", "next", "Next page"),
        ("p", "prev", "Previous page"),
        ("/", "filter", "Filter"),
        ("r", "repl", "REPL"),
    ]

    def __init__(self):
        super().__init__()

        self.table = Table()
        self.table.cursor_type = "row"
        self.table.add_columns("File path", "hash")
        self.results = []
        self.files = []

        self.page_size = DEFAULT_PAGE_SIZE

    def get_path(self) -> str:
        return "files"

    @classmethod
    def get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="", exit_on_error=False)
        parser.add_argument("--prefix", help="Search Files by prefix.")
        parser.add_argument("--page", type=int, default=1, help="Page of File results.")
        return parser

    def watch_args(self) -> None:
        super().watch_args()
        self.call_after_refresh(self.load_files)

    def on_table_selected(self, message: Table.Selected) -> None:
        """
        Callback for when task is selected.
        """
        file = self.files[self.table.cursor_coordinate.row]

        # Use call_after_refresh to prevent the current enter key from acting on next screen.
        self.app.call_after_refresh(self.app.route, [f"values/{file.value_hash}"])

    def action_next(self) -> None:
        """
        Callback for next page of jobs.
        """
        self.args.page += 1
        self.query_one(RedunFooter).page = self.args.page
        self.watch_args()

    def action_prev(self) -> None:
        """
        Callback for previous page of jobs.
        """
        if self.args.page > 1:
            self.args.page -= 1
            self.query_one(RedunFooter).page = self.args.page
            self.watch_args()

    def action_repl(self) -> None:
        """
        Callback for requesting a REPL to explore the execution namespaces.
        """
        screen = self.app.get_screen("ReplScreen")
        screen.update(
            {
                "values": self.files,
                "files": [File(file.path) for file in self.files],
            }
        )
        self.app.push_screen(screen)

    async def load_files(self):
        """
        Load task fullnames from db.
        """
        query = self.app.session.query(DbFile)

        if self.args.prefix:
            query = query.filter(
                DbFile.path >= self.args.prefix,
                DbFile.path < self.args.prefix + chr(255),
            ).filter(DbFile.path.like(self.args.prefix + "%"))

        query = (
            query.order_by(DbFile.path)
            .offset((self.args.page - 1) * self.page_size)
            .limit(self.page_size)
        )

        self.files = list(query.all())

        # Clear table.
        self.table.clear()
        for file in self.files:
            self.table.add_row(file.path, file.value_hash)

    def compose(self) -> ComposeResult:
        self.table.focus()

        yield Container(
            self.header,
            self.table,
            RedunFooter(page=self.args.page),
        )


class TasksScreen(RedunScreen):
    """
    Top-level Screen for exploring all redun Tasks.
    """

    path_pattern = r"^tasks$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
        ("/", "filter", "Filter"),
        ("r", "repl", "REPL"),
    ]

    def __init__(self):
        super().__init__()

        self.table = Table()
        self.table.cursor_type = "row"
        self.table.add_columns("Task", "Versions")
        self.results = []
        self.tasks = []

    def get_path(self) -> str:
        return "tasks"

    @classmethod
    def get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="", exit_on_error=False)
        parser.add_argument("--find", help="Search tasks by name substring.")
        parser.add_argument("--namespace", help="Search tasks by namespace prefix.")
        return parser

    def watch_args(self) -> None:
        super().watch_args()
        self.call_after_refresh(self.load_tasks)

    def on_table_selected(self, message: Table.Selected) -> None:
        """
        Callback for when task is selected.
        """
        task_name = self.tasks[self.table.cursor_coordinate.row]

        # Use call_after_refresh to prevent the current enter key from acting on next screen.
        self.app.call_after_refresh(self.app.route, [f"tasks/name/{task_name}"])

    def action_repl(self) -> None:
        """
        Callback for requesting a REPL to explore the execution namespaces.
        """
        screen = self.app.get_screen("ReplScreen")
        screen.update(
            {
                "tasks": self.tasks,
                "results": self.results,
            }
        )
        self.app.push_screen(screen)

    async def load_tasks(self):
        """
        Load task fullnames from db.
        """
        query = (
            self.app.session.query(Task.namespace + "." + Task.name, sa.func.count(Task.hash))
            .group_by(Task.namespace, Task.name)
            .order_by(Task.namespace, Task.name)
        )

        if self.args.find:
            query = query.filter((Task.namespace + "." + Task.name).like(f"%{self.args.find}%"))
        if self.args.namespace:
            query = query.filter(Task.namespace.like(self.args.namespace + "%"))

        self.results = list(query.all())
        self.tasks = [name.strip(".") for name, _ in self.results]

        # Clear table.
        self.table.clear(columns=True)
        self.table.add_columns(f"Task ({len(self.tasks)})", "Versions")
        for task_fullname, count in self.results:
            self.table.add_row(task_fullname.strip("."), count)

    def compose(self) -> ComposeResult:
        self.table.focus()

        yield Container(
            self.header,
            self.table,
            Footer(),
        )


class TaskVersionsScreen(RedunScreen):
    """
    Screen for exploring versions of a redun Task.
    """

    path_pattern = r"^tasks/name/([^/]*)$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
        ("r", "repl", "REPL"),
    ]

    def __init__(self, task_fullname: str):
        self.task_fullname = task_fullname
        super().__init__()

        self.table = Table()
        self.table.cursor_type = "row"
        self.table.add_columns("Task", "Version", "Jobs", "Most recent job")
        self.results: List = []
        self.tasks: List[Task] = []

    def get_path(self) -> str:
        return f"tasks/name/{self.task_fullname}"

    def watch_args(self) -> None:
        super().watch_args()
        self.call_after_refresh(self.load_tasks)

    def on_table_selected(self, message: Table.Selected) -> None:
        """
        Callback for when task is selected.
        """
        task = self.tasks[self.table.cursor_coordinate.row]

        # Use call_after_refresh to prevent the current enter key from acting on next screen.
        self.app.call_after_refresh(self.app.route, [f"tasks/{task.hash}"])

    def action_repl(self) -> None:
        """
        Callback for requesting a REPL to explore the execution namespaces.
        """
        screen = self.app.get_screen("ReplScreen")
        screen.update(
            {
                "tasks": self.tasks,
                "results": self.results,
            }
        )
        self.app.push_screen(screen)

    async def load_tasks(self):
        """
        Load task fullnames from db.
        """
        namespace, name = split_task_fullname(self.task_fullname)

        self.results = list(
            self.app.session.query(Task, sa.func.count(Job.id), sa.func.max(Job.start_time))
            .join(Job, Job.task_hash == Task.hash)
            .filter(Task.namespace == namespace, Task.name == name)
            .group_by(Task.hash)
            .order_by(sa.func.max(Job.start_time).desc())
            .all()
        )
        self.tasks = [task for task, _, _ in self.results]

        # Clear table.
        self.table.clear(columns=True)
        self.table.add_columns(f"Task ({len(self.tasks)})", "Version", "Jobs", "Most recent job")
        for task, jobs, start_time in self.results:
            self.table.add_row(
                task.fullname, task.hash, jobs, start_time.strftime("%Y-%m-%d %H:%M:%S")
            )

    def compose(self) -> ComposeResult:
        self.table.focus()

        yield Container(
            self.header,
            self.table,
            Footer(),
        )


class TaskScreen(RedunScreen):
    """
    Screen for exploring a redun Task.
    """

    path_pattern = r"^tasks/([^/]*)$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "back", "Back"),
        ("r", "repl", "REPL"),
    ]

    def __init__(self, task_hash: str):
        self.task_hash = task_hash
        super().__init__()
        self.redun_task: Optional[Task] = self.app.session.get(Task, task_hash)

        if self.redun_task:
            self.versions: int = (
                self.app.session.query(Task)
                .filter(
                    Task.namespace == self.redun_task.namespace, Task.name == self.redun_task.name
                )
                .count()
            )
        else:
            self.versions = 0

    def get_path(self) -> str:
        return f"tasks/{self.task_hash}"

    def action_repl(self) -> None:
        """
        Callback for requesting a repl for the current Task.
        """
        if not self.redun_task:
            return
        screen = self.app.get_screen("ReplScreen")
        screen.update({"task": self.redun_task}, obj_id=self.redun_task.hash)
        self.app.push_screen(screen)

    def action_click_namespace(self) -> None:
        """
        Callback for clicking Task namespace.
        """
        assert self.redun_task
        self.app.route(["tasks", "--namespace", self.redun_task.namespace])

    def action_click_versions(self) -> None:
        """
        Callback for clicking on Task versions link.
        """
        assert self.redun_task
        self.app.route([f"tasks/name/{self.redun_task.fullname}"])

    def action_click_jobs(self) -> None:
        """
        Callback for clicking jobs link.
        """
        self.app.route(["search", "--job", "--task-hash", self.task_hash])

    def compose(self) -> ComposeResult:
        if not self.redun_task:
            # Unknown Task screen.
            yield Container(
                self.header,
                Static(f"[bold red]Unknown Task {self.task_hash}[/]"),
                Footer(),
            )
            return

        njobs = self.app.session.query(Job).filter(Job.task_hash == self.task_hash).count()

        if self.redun_task.namespace:
            display_name = f"[@click=screen.click_namespace]{self.redun_task.namespace}[/].{self.redun_task.name}"  # noqa: E501
        else:
            display_name = self.redun_task.name

        yield Container(
            self.header,
            Container(
                Static(f"[bold]Task[/] {self.redun_task.hash} [bold]{display_name}[/]"),
                Static(f"[bold]Versions:[/] [@click=screen.click_versions]{self.versions}[/]"),
                Static(),
                Static(f"  [@click=screen.click_jobs]Jobs {njobs}[/]"),
                Static(),
                Static("[bold]Task source[/]"),
                Static(
                    Syntax(
                        code=self.redun_task.source,
                        lexer="Python",
                        line_numbers=True,
                        word_wrap=False,
                        indent_guides=True,
                        # theme="github-dark",
                    )
                ),
                id="task-screen",
            ),
            Footer(),
        )


class JobScreen(RedunScreen):
    """
    Screen for exploring a redun Job.
    """

    path_pattern = r"^jobs/(.*)$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "back", "Back"),
        ("p", "parent", "Parent job"),
        ("c", "children", "Child jobs"),
        ("r", "repl", "REPL"),
    ]

    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__()

        self.job = (
            self.app.session.query(Job)
            .options(
                joinedload(Job.call_node).joinedload(CallNode.arguments).joinedload(Argument.value)
            )
            .get(job_id)
        )

    def get_path(self) -> str:
        return f"jobs/{self.job_id}"

    def action_parent(self) -> None:
        """
        Callback for showing parent job.
        """
        assert self.job
        if self.job.parent_id:
            self.app.route([f"jobs/{self.job.parent_id}"])
        else:
            self.app.route([f"executions/{self.job.execution_id}"])

    def action_children(self) -> None:
        """
        Callback for showing child jobs.
        """
        assert self.job
        self.app.route(
            [
                f"executions/{self.job.execution_id}",
                "--job",
                self.job_id,
            ]
        )

    def action_repl(self) -> None:
        """
        Callback for requesting a repl for the current Job.
        """
        if not self.job:
            return

        screen = self.app.get_screen("ReplScreen")
        locals: Dict[str, Any] = {
            "job": self.job,
        }
        if self.job.call_node:
            arguments = self.job.call_node.arguments
            args = sorted(
                [arg for arg in arguments if arg.arg_position is not None],
                key=lambda arg: arg.arg_position,
            )
            args = [arg.value.preview for arg in args]
            kwargs = {
                arg.arg_key: arg.value.preview for arg in arguments if arg.arg_key is not None
            }
            locals["args"] = args
            locals["kwargs"] = kwargs
            locals["result"] = self.job.call_node.value.preview
        screen.update(locals, obj_id=self.job.id)
        self.app.push_screen(screen)

    def action_click_exec(self) -> None:
        """
        Callback for clicking on Execution.
        """
        assert self.job
        self.app.route([f"executions/{self.job.execution_id}"])

    def action_click_parent_job(self) -> None:
        """
        Callback for clicking on the parent job.
        """
        assert self.job
        self.app.route([f"jobs/{self.job.parent_id}"])

    def action_click_value(self, value_hash: str) -> None:
        """
        Callback for clicking on a Value.
        """
        self.app.route([f"values/{value_hash}"])

    def action_click_task(self) -> None:
        """
        Callback for click on a Task.
        """
        assert self.job
        self.app.route([f"tasks/{self.job.task_hash}"])

    def compose(self) -> ComposeResult:
        if not self.job:
            # Unknown job screen.
            yield Container(
                self.header,
                Static(f"[red bold]Unknown Job {self.job_id}[/]"),
                Footer(),
            )
            return

        start_time = self.job.start_time.strftime("%Y-%m-%d %H:%M:%S")
        duration = format_timedelta(self.job.duration) if self.job.duration else "Unknown"

        if self.job.call_node:
            arguments = sorted(
                self.job.call_node.arguments,
                key=lambda arg: (
                    arg.arg_position if arg.arg_position is not None else 99,
                    arg.arg_key or "",
                ),
            )
            args = [
                ValueSpan(
                    arg.value,
                    f"  {arg.arg_key or arg.arg_position} = {{value}}",
                    on_click=self.action_click_value,
                )
                for arg in arguments
            ]

            if self.job.status == "FAILED":
                error_value = self.job.call_node.value.value_parsed
                error = error_value if isinstance(error_value, ErrorValue) else "Unknown"
                result = Static(f"[bold]Raised:[/] {error}")

                lines = ["", "[bold]Traceback:[/]"]
                if isinstance(error_value, ErrorValue) and error_value.traceback:
                    for line in error_value.traceback.format():
                        lines.append(line.rstrip("\n"))
                traceback = [Static("\n".join(lines))]
            else:
                result = ValueSpan(
                    self.job.call_node.value,
                    "[bold]Result:[/] {value}",
                    on_click=self.action_click_value,
                )
                traceback = []

        else:
            args = []
            result = Static("[bold]Result:[/b] Unknown")
            traceback = []

        source = self.job.task.source

        yield Container(
            self.header,
            Container(
                Static(
                    f"[bold]Job[/] {self.job.id} {style_status(self.job.status)} "
                    f"{start_time} [bold]{self.job.task.fullname}[/]"
                ),
                Static(format_traceback(self.job)),
                Static(f"[bold]Duration:[/] {duration}"),
                Static("[bold]Exec Tags:[/] " + format_tags(self.job.execution.tags)),
                Static("[bold]Tags:[/] " + format_tags(self.job.tags)),
                TagLinks(self.app.link_patterns, self.job.execution.tags + self.job.tags),
                Static(),
                Static(f"[bold]CallNode:[/] {self.job.call_hash}"),
                Static("[bold]Args:[/]"),
                Vertical(*args, id="job-args-list"),
                result,
                Vertical(*traceback, id="job-traceback"),
                Static(),
                Static(
                    f"[@click=screen.click_task][bold]Task[/] {self.job.task_hash} "
                    f"[bold]{self.job.task.fullname}[/][/]"
                ),
                Static(
                    Syntax(
                        code=source,
                        lexer="Python",
                        line_numbers=True,
                        word_wrap=False,
                        indent_guides=True,
                        # theme="github-dark",
                    )
                ),
                id="job-screen",
            ),
            Footer(),
        )


def tree_sort_jobs(root: Job, jobs: List[Job]) -> List[Job]:
    """
    Sort Jobs in preorder traversal. Annotate tree depth on each job.
    """
    job2children = defaultdict(list)

    # Assume jobs are already sorted by start_time.
    for job in jobs:
        job2children[job.parent_id].append(job)

    def walk(job: Job, depth: int) -> Iterator[Job]:
        job.depth = depth
        yield job
        for child in job2children[job.id]:
            yield from walk(child, depth + 1)

    return list(walk(root, 0))


class ExecutionScreen(RedunScreen):
    """
    Screen for exploring a redun Execution.
    """

    path_pattern = r"^executions/(.*)$"

    jobs = reactive(None)

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "back", "Back"),
        ("n", "next", "Next page"),
        ("p", "prev", "Previous page"),
        ("f", "focus", "Focus job"),
        ("g", "unfocus", "Unfocus job"),
        ("/", "filter", "Filter"),
        ("r", "repl", "REPL"),
        ("ctrl+r", "refresh", "Refresh"),
    ]

    def __init__(self, execution_id: str):
        self.execution_id = execution_id
        super().__init__()

        self.execution = self.app.session.get(Execution, execution_id)

        self.page_size = DEFAULT_PAGE_SIZE

        self.header = RedunHeader(f"executions/{self.execution_id}")
        self.job_list = JobList(None)
        self.job_status_list = JobStatusTable(execution_id)

    def get_path(self) -> str:
        return f"executions/{self.execution_id}"

    @classmethod
    def get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="", exit_on_error=False)
        parser.add_argument("--job", help="Focus list to job and its children.")
        parser.add_argument(
            "--status",
            default=[],
            action="append",
            help="Filter jobs by status (DONE, FAILED, CACHED).",
        )
        parser.add_argument(
            "--task", default=[], action="append", help="Filter jobs by task name."
        )
        parser.add_argument("--page", default=1, type=int, help="Jobs page to display.")
        return parser

    def watch_args(self) -> None:
        """
        Update UI when args change.
        """
        super().watch_args()
        self.call_after_refresh(self.load_jobs)

    def action_back(self):
        """
        Callback for closing screen.
        """
        self.app.close_screen()

    def action_next(self) -> None:
        """
        Callback for next page of jobs.
        """
        self.args.page += 1
        self.query_one(RedunFooter).page = self.args.page
        self.watch_args()

    def action_prev(self) -> None:
        """
        Callback for previous page of jobs.
        """
        if self.args.page > 1:
            self.args.page -= 1
            self.query_one(RedunFooter).page = self.args.page
            self.watch_args()

    def action_focus(self) -> None:
        """
        Callback for focusing on currently selected job.
        """
        if self.job_list.jobs:
            job = self.job_list.jobs[self.job_list.cursor_coordinate.row]
            self.args.job = job.id
            self.args.page = 1
            self.watch_args()

    def action_unfocus(self) -> None:
        """
        Callback for unfocusing the focused job.
        """
        self.args.job = None
        self.watch_args()

    def action_repl(self) -> None:
        """
        Callback for requesting a REPL to explore the execution.
        """
        if not self.execution:
            return
        tasks = {job.task.fullname: job.task for job in self.execution.jobs}

        screen = self.app.get_screen("ReplScreen")
        screen.update(
            {
                "exec": self.execution,
                "tasks": tasks,
            },
            obj_id=self.execution.id,
        )
        self.app.push_screen(screen)

    async def action_refresh(self) -> None:
        await self.load_jobs()

    async def load_jobs(self):
        if not self.execution:
            return

        page = self.args.page

        if not self.args.job:
            # Show job tree.
            show_tree = True

            query = self.app.session.query(Job).filter(Job.execution_id == self.execution_id)

            for task_fullname in self.args.task:
                show_tree = False
                namespace, name = split_task_fullname(task_fullname)
                query = query.join(Task, Job.task_hash == Task.hash).filter(
                    Task.namespace == namespace, Task.name == name
                )

            for status in self.args.status:
                show_tree = False
                if status == "FAILED":
                    query = query.filter(Job.end_time.is_(None))
                elif status == "CACHED":
                    query = query.filter(Job.cached.is_(True))
                elif status == "DONE":
                    query = query.filter(Job.end_time.isnot(None) & Job.cached.is_(False))

            if show_tree:
                # Figure out sorting and depth in job tree.
                all_jobs = tree_sort_jobs(self.execution.job, self.execution.jobs)
            else:
                all_jobs = list(query.all())

                # Assign default depth.
                for job in all_jobs:
                    job.depth = 0

            job2index = {job.id: i for i, job in enumerate(all_jobs)}

            # Determine job ids within current page.
            page_job_ids = [
                job.id for job in all_jobs[(page - 1) * self.page_size : page * self.page_size]
            ]

            # Fetch page worth of jobs from db.
            self.jobs = sorted(
                query.filter(Job.id.in_(page_job_ids))
                .options(
                    joinedload(Job.task),
                    joinedload(Job.call_node)
                    .joinedload(CallNode.arguments)
                    .joinedload(Argument.value),
                )
                .order_by(Job.start_time)
                .all(),
                key=lambda x: job2index[x.id],
            )

        else:
            # Show immediate child jobs.
            root_id = self.args.job

            root = self.app.session.query(Job).filter(Job.id == root_id).all()
            children = (
                self.app.session.query(Job)
                .filter(Job.parent_id == root_id)
                .options(
                    joinedload(Job.task),
                    joinedload(Job.call_node)
                    .joinedload(CallNode.arguments)
                    .joinedload(Argument.value),
                )
                .order_by(Job.start_time)
                .offset((page - 1) * self.page_size)
                .limit(self.page_size)
                .all()
            )

            self.jobs = root + children
            if root:
                # Annotate depth for display.
                tree_sort_jobs(root[0], children)

        self.job_list.jobs = self.jobs

    def on_job_list_selected(self, message: JobList.Selected) -> None:
        self.app.goto_screen(JobScreen, message.job.id, (message.job.id,))

    def on_table_selected(self, message: Table.Selected) -> None:
        if message.table == self.job_status_list:
            coord = self.job_status_list.cursor_coordinate

            if coord.column == 0:
                # Task selected.
                task = self.job_status_list.tasks[coord.row]
                if task:
                    self.call_after_refresh(self.app.route, [f"tasks/{task.hash}"])

            else:
                # A status is selected.
                status = SchedulerJob.STATUSES[coord.column - 1]
                if status == "TOTAL":
                    self.args.status = []
                else:
                    self.args.status = [status]

                if coord.row > 0:
                    # A task is selected.
                    self.args.task = [self.job_status_list.tasks[coord.row].fullname]
                else:
                    self.args.task = []

            self.args.job = None
            self.notify_args()

    def compose(self) -> ComposeResult:
        if not self.execution:
            # Unknown Execution screen.
            yield Container(
                self.header,
                Static(f"[red bold]Unknown Execution {self.execution_id}[/]"),
                RedunFooter(self.args.page),
            )
            return

        self.job_list.focus()

        start_time = self.execution.job.start_time.strftime("%Y-%m-%d %H:%M:%S")
        args = " ".join(json.loads(self.execution.args)[1:])

        yield Container(
            self.header,
            Static(
                f"[bold]Execution[/] {self.execution.id} {style_status(self.execution.status)} "
                f"{start_time}: {args}",
                id="execution-title",
            ),
            Static("[bold]Tags:[/] " + format_tags(self.execution.tags)),
            TagLinks(self.app.link_patterns, self.execution.tags),
            Static(),
            Static("Job statuses", classes="header"),
            self.job_status_list,
            Static(),
            Static(f"Jobs ({len(self.execution.jobs)})", classes="header"),
            self.job_list,
            Static(),
            RedunFooter(self.args.page),
        )


class ExecutionsNamespaceScreen(RedunScreen):
    """
    Top-level Screen for exploring all redun Execution namespaces.
    """

    path_pattern = r"^executions/namespace$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "app.pop_screen()", "Back"),
        ("n", "sort('namespace')", "Sort by namespace"),
        ("c", "sort('count')", "Sort by Execution count"),
        ("t", "sort('time')", "Sort by 'most recent'"),
        ("/", "filter", "Filter"),
        ("r", "repl", "REPL"),
    ]

    def __init__(self):
        super().__init__()

        self.table = Table()
        self.table.cursor_type = "row"
        self.table.add_columns("Execution namespace", "Executions", "Most recent")
        self.results = []
        self.namespaces = []

    def get_path(self) -> str:
        return "executions/namespace"

    @classmethod
    def get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="", exit_on_error=False)
        parser.add_argument(
            "--sort",
            default="time",
            choices=["namespace", "count", "time"],
            help="Sort Execution namespaces.",
        )
        return parser

    def on_mount(self):
        super().on_mount()
        self.call_after_refresh(self.load_namespaces)

    def watch_args(self):
        super().watch_args()
        self.call_after_refresh(self.load_namespaces)

    def on_table_selected(self, message: Table.Selected) -> None:
        """
        Callback for when namespace is selected.
        """
        self.app.route(
            ["executions", "--namespace", self.namespaces[self.table.cursor_coordinate.row]]
        )

    def action_sort(self, column: str) -> None:
        """
        Callback for selecting column sorting.
        """
        self.args.sort = column
        self.notify_args()

    def action_repl(self) -> None:
        """
        Callback for requesting a REPL to explore the execution namespaces.
        """
        screen = self.app.get_screen("ReplScreen")
        screen.update(
            {
                "namespaces": self.namespaces,
                "results": self.results,
            }
        )
        self.app.push_screen(screen)

    async def load_namespaces(self):
        """
        Load execution namespaces list from db.
        """
        query = (
            self.app.session.query(
                Task.namespace, sa.func.count(Execution.id), sa.func.max(Job.start_time)
            )
            .join(Job, Job.task_hash == Task.hash)
            .join(Execution, Execution.job_id == Job.id)
            .group_by(Task.namespace)
        )

        if self.args.sort == "namespace":
            query = query.order_by(Task.namespace)
        elif self.args.sort == "count":
            query = query.order_by(sa.func.count(Execution.id).desc())
        elif self.args.sort == "time":
            query = query.order_by(sa.func.max(Job.start_time).desc())

        self.results = list(query.all())
        self.namespaces = [ns for ns, _, _ in self.results]

        self.table.clear()
        for namespace, count, start_time in self.results:
            self.table.add_row(namespace, count, start_time.strftime("%Y-%m-%d %H:%M:%S"))

    def compose(self) -> ComposeResult:
        self.table.focus()

        yield Container(
            self.header,
            self.table,
            Footer(),
        )


class ExecutionsScreen(RedunScreen):
    """
    Top-level Screen for exploring all redun Executions.
    """

    path_pattern = "^executions$"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "back", "Back"),
        ("n", "next", "Next page"),
        ("p", "prev", "Previous page"),
        ("/", "filter", "Filter"),
        ("r", "repl", "REPL"),
        ("ctrl+r", "refresh", "Refresh"),
    ]

    def __init__(self):
        super().__init__()

        self.page_size = self.app.size[1] - 6  # Subtract header and footer heights.

        self.execution_list = ExecutionList(None)

    def get_path(self) -> str:
        return "executions"

    @classmethod
    def get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="", exit_on_error=False)
        parser.add_argument("--id", help="Filter by Execution id prefix.")
        parser.add_argument("--namespace", help="Filter by root task namespace prefixes.")
        parser.add_argument(
            "--find", help="Filter by root task namespace and argument substrings."
        )
        parser.add_argument("--page", type=int, default=1, help="Executions page to display.")
        return parser

    def notify_args(self) -> None:
        if self.is_current:
            # Only process args update when a current screen.
            # This extra logic is needed since ExecutionsScreen is always the
            # default screen, but may not be the first displayed.
            super().notify_args()

    def watch_args(self) -> None:
        """
        Update filter when args change.
        """
        super().watch_args()

        # Reload executions.
        self.execution_list.executions = None
        self.call_after_refresh(self.load_executions)

    async def action_refresh(self) -> None:
        await self.load_executions()

    def on_screen_resume(self, message: Message) -> None:
        """
        When resuming the screen ensure executions are loaded.
        """
        self.notify_args()

    def action_back(self) -> None:
        # The first two screens should never be popped.
        if len(self.app.screen_stack) > 2:
            self.app.pop_screen()

    def action_next(self) -> None:
        """
        Callback for next page of executions.
        """
        self.args.page += 1
        self.query_one(RedunFooter).page = self.args.page
        self.notify_args()

    def action_prev(self) -> None:
        """
        Callback for previous page of executions.
        """
        if self.args.page > 1:
            self.args.page -= 1
            self.query_one(RedunFooter).page = self.args.page
            self.notify_args()

    def action_repl(self) -> None:
        """
        Callback for requesting a repl for the current executions list.
        """
        screen = self.app.get_screen("ReplScreen")
        screen.update(
            {
                "execs": self.execution_list.executions,
            }
        )
        self.app.push_screen(screen)

    def on_execution_list_selected(self, message: ExecutionList.Selected) -> None:
        """
        Callback for when execution is selected.
        """
        self.app.route([f"executions/{message.execution.id}"])

    async def load_executions(self):
        """
        Load executions list from db.
        """
        query = self.app.session.query(Execution)

        if self.args.id:
            query = query.filter(Execution.id.like(self.args.id + "%"))
        if self.args.namespace == "":
            query = query.filter(Task.namespace == "")
        elif self.args.namespace:
            query = query.filter(Task.namespace.like(self.args.namespace + "%"))
        elif self.args.find:
            query = query.filter(
                Task.namespace.like("%" + self.args.find + "%")
                | Execution.args.like("%" + self.args.find + "%")
            )

        self.execution_list.executions = (
            query.options(
                joinedload(Execution.job).joinedload(Job.task),
            )
            .join(Job, Execution.job_id == Job.id)
            .join(Task, Job.task_hash == Task.hash)
            .order_by(Job.start_time.desc())
            .offset((self.args.page - 1) * self.page_size)
            .limit(self.page_size)
            .all()
        )

    def compose(self) -> ComposeResult:
        self.execution_list.focus()

        yield Container(
            self.header,
            self.execution_list,
            RedunFooter(page=self.args.page),
        )
