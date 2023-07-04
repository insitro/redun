import re
from argparse import Namespace
from typing import Any, List, Type, cast

import sqlalchemy as sa
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import Label, ListItem, ListView

from redun.backends.db import CallNode, Execution
from redun.backends.db import File as DbFile
from redun.backends.db import Job, RedunBackendDb, Task, Value
from redun.backends.db.query import infer_id
from redun.console.screens import (
    ExecutionScreen,
    ExecutionsNamespaceScreen,
    ExecutionsScreen,
    FilesScreen,
    JobScreen,
    RedunScreen,
    ReplScreen,
    SearchScreen,
    TaskScreen,
    TasksScreen,
    TaskVersionsScreen,
    ValueScreen,
)
from redun.console.upstream_screen import UpstreamDataflowScreen
from redun.scheduler import Scheduler


class MenuItem(ListItem):
    def __init__(self, widget: Widget, value: Any):
        super().__init__(widget)
        self.value = value


class MenuScreen(Screen):
    """
    Main menu for top-level Screens.
    """

    CSS_PATH = "style.css"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("m", "", ""),
        ("escape", "app.pop_screen()", "Back"),
    ]

    def on_list_view_selected(self, message) -> None:
        self.app.pop_screen()
        self.app.push_screen(message.item.value)

    def compose(self) -> ComposeResult:
        list_view = ListView(
            MenuItem(Label("Executions"), "ExecutionsScreen"),
            MenuItem(Label("Execution namespaces"), "ExecutionsNamespaceScreen"),
            MenuItem(Label("Tasks"), "TasksScreen"),
            MenuItem(Label("Files"), "FilesScreen"),
            MenuItem(Label("Search"), "SearchScreen"),
            MenuItem(Label("REPL"), "ReplScreen"),
            id="menu-list",
        )
        list_view.focus()
        yield list_view


class RedunApp(App):
    """
    Top-level redun console App.
    """

    TITLE = "redun"
    CSS_PATH = "style.css"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("m", "push_screen('MenuScreen')", "Menu"),
        Binding("ctrl+p", "print_screen", "Print screen", show=False),
    ]
    SCREENS = {
        "MenuScreen": MenuScreen,
        "ExecutionsScreen": ExecutionsScreen,
        "ExecutionsNamespaceScreen": ExecutionsNamespaceScreen,
        "TasksScreen": TasksScreen,
        "FilesScreen": FilesScreen,
        "ReplScreen": ReplScreen,
        "SearchScreen": SearchScreen,
    }

    # Screens that support argv routing.
    route_screens: List[Type[RedunScreen]] = [
        ExecutionsScreen,
        ExecutionsNamespaceScreen,
        ExecutionScreen,
        JobScreen,
        TasksScreen,
        TaskVersionsScreen,
        TaskScreen,
        ValueScreen,
        UpstreamDataflowScreen,
        FilesScreen,
        SearchScreen,
    ]

    def __init__(
        self, scheduler: Scheduler, args: Namespace, extra_args: List[str], argv: List[str]
    ):
        super().__init__()

        self.scheduler = scheduler
        self.session = cast(sa.orm.Session, cast(RedunBackendDb, self.scheduler.backend).session)
        assert self.session
        self.args = args
        self.all_argv = argv
        self.argv = extra_args

        self.link_patterns = [
            r"https://github.com/{{git_origin_url:.*github\.com(:|/)(?P<val>.*)\.git$}}/commit/{{git_commit}}",  # noqa: E501
            r"https://console.aws.amazon.com/batch/home?#jobs/detail/{{aws_batch_job}}",
            r"https://console.aws.amazon.com/cloudwatch/home?#logEventViewer:group=/aws/batch/job;stream={{aws_log_stream}}",  # noqa: E501
        ]

    def action_print_screen(self) -> None:
        self.save_screenshot(path="./")

    def on_mount(self) -> None:
        # Default base screen.
        self.push_screen("ExecutionsScreen")

        # If a path is given, route directly to the appropriate screen.
        if self.argv:
            self.route(self.argv)

    def goto_screen(self, screen_cls: Screen, name: str, args: tuple) -> Screen:
        """
        Get or create a new screen.
        """
        name = screen_cls.__name__ + "-" + name
        if self.is_screen_installed(name):
            screen = self.get_screen(name)
        else:
            screen = screen_cls(*args)
            self.install_screen(screen, name)
        self.push_screen(screen)
        return screen

    def close_screen(self) -> None:
        """
        Pop and uninstall the current screen.
        """
        screen = self.pop_screen()
        if screen not in self.app.screen_stack:
            self.uninstall_screen(screen)

    def goto_record(self, record: Any) -> None:
        """
        Push a Screen relevant for the given record.
        """
        if isinstance(record, Execution):
            self.goto_screen(ExecutionScreen, record.id, (record.id,))
        elif isinstance(record, Job):
            self.goto_screen(JobScreen, record.id, (record.id,))
        elif isinstance(record, Task):
            self.goto_screen(TaskScreen, record.hash, (record.hash,))
        elif isinstance(record, Value):
            self.goto_screen(ValueScreen, record.value_hash, (record.value_hash,))
        else:
            screen = self.get_screen("ReplScreen")
            screen.update({"obj": record})
            self.push_screen(screen)

    def route(self, argv: List[str]) -> None:
        """
        Route a path to the appropriate Screen.
        """
        # If no path is given, assume the main executions screen.
        path = argv[0] if argv else "executions"

        # Determine if path represents a record id.
        try:
            obj = infer_id(self.session, path, include_files=False)
        except ValueError:
            obj = None
        if obj:
            # Determine if we can redirect to a supported Screen.
            if isinstance(obj, Execution):
                path = f"executions/{obj.id}"
            elif isinstance(obj, Job):
                path = f"jobs/{obj.id}"
            elif isinstance(obj, CallNode):
                # Redirect to oldest job.
                if obj.jobs:
                    path = f"jobs/{obj.jobs[0].id}"
            elif isinstance(obj, Task):
                path = f"tasks/{obj.hash}"
            elif isinstance(obj, (Value, DbFile)):
                path = f"values/{obj.value_hash}"
            elif isinstance(obj, CallNode):
                path = f"repl/{path}"

        # Determine if path corresponds to a routable screen.
        for screen_cls in self.route_screens:
            groups = re.match(screen_cls.path_pattern, path)
            if groups:
                if self.is_screen_installed(screen_cls.__name__):
                    # Singleton screen that doesn't auto-uninstall.
                    screen = self.get_screen(screen_cls.__name__)
                    screen.parse(argv)
                    self.push_screen(screen)
                else:
                    # Auto-installing custom screen.
                    id = groups[1]
                    screen = self.goto_screen(screen_cls, id, (id,))
                    screen.parse(argv)
                return

        # ReplScreen has its own customization.
        groups = re.match(ReplScreen.path_pattern, path)
        if groups:
            obj_id = groups[2]
            screen = self.get_screen("ReplScreen")
            screen.update(obj_id=obj_id)
            self.push_screen(screen)
            return

        # Redirect for paths that look like File path prefixes.
        file = (
            self.session.query(DbFile.path)
            .filter(DbFile.path >= path)
            .order_by(DbFile.path)
            .first()
        )
        if file and file[0].startswith(path):
            self.route(["files", "--prefix", path])
