from typing import Dict, Iterator, Optional, Tuple

from textual.app import ComposeResult
from textual.containers import Container
from textual.widgets import Footer, Static

from redun.backends.db import CallNode, Value
from redun.backends.db.dataflow import (
    ArgumentValue,
    CallNodeValue,
    DataflowDOM,
    DataflowNode,
    DataflowSectionDOM,
    get_default_arg_name,
    get_node_hash,
    get_task_args,
    make_dataflow_dom,
    walk_dataflow,
)
from redun.console.screens import RedunScreen
from redun.utils import assert_never, trim_string


def display_node(node: Optional[DataflowNode], renames: Dict[str, str]) -> Tuple[str, str]:
    """
    Formats a dataflow node to a string.
    """
    if isinstance(node, CallNode):
        return ("", display_call_node(node, renames))

    elif isinstance(node, ArgumentValue):
        return (
            "argument of",
            display_call_node(node.argument.call_node, renames),
        )

    elif isinstance(node, CallNodeValue):
        return ("", display_call_node(node.call_node, renames))

    elif node is None:
        return ("", "origin")

    else:
        assert_never(node)


def display_call_node(call_node: CallNode, renames: Dict[str, str]) -> str:
    """
    Formats a CallNode to a string.
    """
    try:
        arg_names = get_task_args(call_node.task)
    except SyntaxError:
        arg_names = [get_default_arg_name(i) for i in range(len(call_node.arguments))]
    args = [renames.get(arg, arg) for arg in arg_names]
    return "{task_name}({args})".format(task_name=call_node.task.name, args=", ".join(args))


def display_value(value: Value) -> str:
    """
    Format a Value to a string.
    """
    return trim_string(repr(value.preview).replace("\n", " "))


def display_hash(node: Optional[DataflowNode]) -> str:
    """
    Formats hash for a DataflowNode.
    """
    node_hash = get_node_hash(node)
    if node_hash:
        return f"<{node_hash[:8]}> "
    else:
        return ""


def display_link(hash: Optional[str], text: str) -> str:
    if hash:
        return f"[@click=screen.click_hash('{hash}')]{text}[/]"
    else:
        return text


def display_section(dom: DataflowSectionDOM) -> Iterator[str]:
    """
    Yields lines for displaying a dataflow section DOM.
    """

    # Display assign line.
    assign = dom.assign
    yield "{var_name} [bold]<--[/] {prefix}{link}".format(
        var_name=assign.var_name,
        prefix=assign.prefix + " " if assign.prefix else "",
        link=display_link(
            get_node_hash(assign.node), display_hash(assign.node) + assign.node_display
        ),
    )

    indent = len(assign.var_name) + 1

    # Display routing.
    for routing_def in dom.routing:
        yield "{indent}[bold]<--[/] {prefix}{link}".format(
            indent=" " * indent,
            prefix=routing_def.prefix + " " if routing_def.prefix else "",
            link=display_link(
                get_node_hash(routing_def.node),
                display_hash(routing_def.node) + routing_def.node_display,
            ),
        )

    # Display argument definitions.
    if dom.args:
        max_var_len = max(len(arg.var_name) for arg in dom.args)
        for arg in dom.args:
            yield "  {var_name}{padding} [bold]=[/] {link}".format(
                var_name=arg.var_name,
                padding=" " * (max_var_len - len(arg.var_name)),
                link=display_link(
                    arg.value.value_hash,
                    display_hash(arg.value) + display_value(arg.value),
                ),
            )


def display_dataflow(dom: DataflowDOM) -> Iterator[str]:
    """
    Yields for lines displaying a dataflow DOM.
    """
    for dom_section in dom:
        yield from display_section(dom_section)
        yield ""


class UpstreamDataflowScreen(RedunScreen):
    """
    Screen for exploring the upstream dataflow of a redun Value.
    """

    path_pattern = "^upstreams/(.*)$"

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
        return f"upstreams/{self.value_hash}"

    def action_click_value(self, value_hash: str) -> None:
        self.app.route([f"values/{value_hash}"])

    def action_click_hash(self, hash: str) -> None:
        self.app.route([hash])

    def action_repl(self) -> None:
        screen = self.app.get_screen("ReplScreen")
        screen.update({"value": self.value.value_parsed}, obj_id=self.value_hash)
        self.app.push_screen(screen)

    def compose(self) -> ComposeResult:
        if self.value is None:
            yield Container(
                self.header,
                Static(f"[bold red]Unknown value {self.value_hash}[/]"),
                Footer(),
            )
            return

        edges = walk_dataflow(self.app.scheduler.backend, self.value)
        dom = make_dataflow_dom(edges, new_varname="value")
        lines = display_dataflow(dom)

        yield Container(
            self.header,
            Container(
                Static(f"[bold]Upstream dataflow[/] {self.value_hash}"),
                Static(),
                Static(
                    f"[bold]value [b]=[/] [@click=screen.click_value('{self.value_hash}')]{display_hash(self.value)}{repr(self.value.preview)}[/]",  # noqa: E501
                    classes="dataflow",
                ),
                Static(),
                Static("\n".join(lines), classes="dataflow"),
                id="upstreams-screen",
            ),
            Footer(),
        )
