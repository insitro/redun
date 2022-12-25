import os.path
import sys

import pygraphviz as pgv

import redun.visualize
from redun import File
from redun.backends.db import CallNode, Execution, Job, RedunBackendDb, Value
from redun.backends.db.query import CallGraphQuery
from redun.cli import RedunClient
from redun.visualize import MAX_LABEL_LEN, SEPARATOR

DEFAULT_WORKFLOW = """
from redun import task

@task()
def task1(x: str, y: str):
    return x + y

@task()
def main(x: str = "hi", y: str = "bye"):
    return task1(x, y)
"""


def run_workflow(argv, file_contents=DEFAULT_WORKFLOW):
    sys.modules.pop("workflow", None)
    file = File("workflow.py")
    file.write(file_contents)

    client = RedunClient()
    client.execute(argv)

    parser = client.get_command_parser()
    args, _ = parser.parse_known_args(argv[1:])

    backend = client.get_backend(args)
    assert isinstance(backend, RedunBackendDb)
    exec_id = (
        backend.session.query(Execution)
        .join(Job, Job.id == Execution.job_id)
        .order_by(Job.start_time.desc())
        .first()
        .id
    )
    return client, exec_id, file


def test_basic_viz():
    file_contents = """
from redun import task

@task()
def main(x: str = "hi", y: bool = True):
    return (x, y)
"""

    long_hi = "hi" * 100
    argv = ["redun", "run", "workflow.py", "main", "--x", long_hi]

    client, exec_id, file = run_workflow(argv, file_contents)

    viz_argv = [
        "redun",
        "viz",
        exec_id,
    ]
    client.execute(viz_argv)

    dot_file, image = File(f"{exec_id}.dot"), File(f"{exec_id}.png")
    graph = pgv.AGraph(dot_file.read())

    query = CallGraphQuery(client.scheduler.backend.session)
    query = query.filter_types({"CallNode", "Value"}).filter_execution_ids([exec_id])

    call_node, result = None, None
    for record in query.all():
        if isinstance(record, CallNode):
            call_node = record
        elif record.value_parsed == (long_hi, True):
            result = record

    arguments = [str((arg.value, arg.arg_hash)) for arg in call_node.arguments]
    result, call_node = str((result, [call_node])), str(call_node)

    expected_nodes = {call_node, result}
    expected_nodes.update(arguments)

    # If the nodes match, adding the graph nodes should have no effect
    produced_nodes = graph.nodes()
    expected_nodes.update(produced_nodes)
    assert len(expected_nodes) == 4

    # Checking truncation
    for node in produced_nodes:
        assert len(node.attr["label"]) <= MAX_LABEL_LEN + len(SEPARATOR)

    produced_edges = graph.edges()
    # Checking twice because there may be duplicate edges that disappear when added to the set
    assert len(produced_edges) == 3
    expected_edges = {(call_node, result), (arguments[0], call_node), (arguments[1], call_node)}
    expected_edges.update(produced_edges)
    assert len(expected_edges) == 3

    # Checking direction
    assert graph.graph_attr["rankdir"] == "TB"

    file.remove()
    dot_file.remove()
    image.remove()


def test_viz_flags():
    x, y = "hi" * 100, "bye" * 100
    result_value = x + y
    argv = ["redun", "run", "workflow.py", "main", "--x", x, "--y", y]
    client, exec_id, file = run_workflow(argv=argv)

    viz_argv = [
        "redun",
        "viz",
        "--hash",
        "--format=dot",
        "--output=test",
        "--horizontal",
        "--jobs",
        "--no-truncation",
        exec_id,
    ]
    client.execute(viz_argv)

    # Testing that the --format and --output specifiers work
    assert not os.path.exists("test.png")
    dot_file = File("test.dot")
    graph = pgv.AGraph(dot_file.read())

    # Checking --horizontal
    assert graph.graph_attr["rankdir"] == "LR"

    query = CallGraphQuery(client.scheduler.backend.session)
    query = query.filter_types({"CallNode", "Value", "Job"}).filter_execution_ids([exec_id])

    call_nodes, jobs, result = [], [], None
    expected_edges = set()
    for record in query.all():
        if isinstance(record, CallNode):
            call_nodes.append(record)
        elif isinstance(record, Job):
            job = str(record)
            jobs.append(job)
            expected_edges.add((job, str(record.call_node)))
            for child_job in record.child_jobs:
                expected_edges.add((job, str(child_job)))
        elif record.value_parsed == result_value:
            result = record

    # Checking --jobs
    assert len(jobs) == 2

    expected_nodes = set(jobs)

    for call_node in call_nodes:
        call_node_str = str(call_node)
        for arg in call_node.arguments:
            arg_str = str((arg.value, arg.arg_hash))
            expected_nodes.add(arg_str)
            expected_edges.add((arg_str, call_node_str))

        for child in call_node.children:
            expected_edges.add((call_node_str, str(child)))

        result_node = str((result, [call_node]))
        expected_nodes.add(result_node)
        expected_nodes.add(call_node_str)
        expected_edges.add((call_node_str, result_node))

    produced_nodes = graph.nodes()
    expected_nodes.update(produced_nodes)
    assert len(expected_nodes) == 10

    produced_edges = graph.edges()
    assert len(expected_edges) == 10
    expected_edges.update(produced_edges)
    assert len(expected_edges) == 10

    # Checking --no-truncation and --hash
    for node in produced_nodes:
        label = node.attr["label"]
        if "Value" in label:
            assert len(label) > MAX_LABEL_LEN
            assert len(label.splitlines()[0].split(" ")) == 2

    file.remove()
    dot_file.remove()


def test_deduplication():
    argv = [
        "redun",
        "run",
        "workflow.py",
        "main",
    ]
    client, exec_id, file = run_workflow(argv)

    result_val = "hibye"
    viz_argv = ["redun", "viz", exec_id, "--format=dot", "--deduplicate"]
    client.execute(viz_argv)

    query = CallGraphQuery(client.scheduler.backend.session)
    query = query.filter_types({"CallNode", "Value"}).filter_execution_ids([exec_id])

    expected_nodes, expected_edges = set(), set()
    call_nodes, result, args = [], None, []
    for record in query.all():
        if isinstance(record, CallNode):
            call_nodes.append(record)
        elif record.value_parsed == result_val:
            result = str((record, None))
        else:
            args.append(str((record, None)))

    for call_node in call_nodes:
        call_node_str = str(call_node)
        for child in call_node.children:
            expected_edges.add((call_node_str, str(child)))

        for arg in args:
            expected_edges.add((arg, call_node_str))

        expected_edges.add((call_node_str, result))

        expected_nodes.add(call_node_str)

    expected_nodes.update(args)
    expected_nodes.add(result)

    dot_file = File(f"{exec_id}.dot")
    graph = pgv.AGraph(dot_file.read())

    expected_nodes.update(graph.nodes())
    assert len(expected_nodes) == 5

    produces_edges = graph.edges()
    assert len(produces_edges) == 7
    expected_edges.update()
    assert len(expected_edges) == 7

    file.remove()
    dot_file.remove()


def test_dataflow():
    file_contents = """
from redun import task, File

@task()
def task1(x):
    return [x["first"], x["second"]] + x["list"]

@task()
def main(x=[File("hi"), File("bye")]):
    return task1({"list": x, "first": x[0], "second": x[1]})
"""

    input = [File("hi"), File("bye")]
    result = input + input
    argv = [
        "redun",
        "run",
        "workflow.py",
        "main",
    ]
    client, exec_id, file = run_workflow(argv, file_contents)

    # Running exec WITHOUT dataflow
    viz_argv = ["redun", "viz", "--format=dot", exec_id]

    client.execute(viz_argv)

    query = CallGraphQuery(client.scheduler.backend.session)
    query = query.filter_types({"CallNode", "Value"}).filter_execution_ids([exec_id])

    expected_nodes, expected_edges = set(), set()
    call_nodes, result, args = [], None, []
    for record in query.all():
        if isinstance(record, CallNode):
            call_nodes.append(record)

    for call_node in call_nodes:
        call_node_str = str(call_node)

        for child in call_node.children:
            expected_edges.add((call_node_str, str(child)))

        for arg in call_node.arguments:
            upstream = arg.upstream if arg.upstream else arg.arg_hash
            arg_str = str((arg.value, upstream))
            expected_nodes.add(arg_str)
            expected_edges.add((arg_str, call_node_str))

        result = str((call_node.value, [call_node]))
        expected_nodes.add(result)
        expected_edges.add((call_node_str, result))

        expected_nodes.add(call_node_str)

    expected_nodes.update(args)
    expected_nodes.add(result)

    dot_file = File(f"{exec_id}.dot")
    graph = pgv.AGraph(dot_file.read())

    expected_nodes.update(graph.nodes())
    assert len(expected_nodes) == 6

    produced_edges = graph.edges()
    assert len(produced_edges) == 5
    expected_edges.update(produced_edges)
    assert len(expected_edges) == 5

    # Running exec WITH dataflow
    viz_argv = ["redun", "viz", "--dataflow", "--format=dot", exec_id]

    client.execute(viz_argv)

    expected_nodes, expected_edges = set(), set()
    call_nodes, values = [], []
    for record in query.all():
        if isinstance(record, CallNode):
            call_nodes.append(record)
        elif isinstance(record, Value) and not record.children:
            values.append(record)

    for call_node in call_nodes:
        call_node_str = str(call_node)

        for child in call_node.children:
            expected_edges.add((call_node_str, str(child)))

        for arg in call_node.arguments:
            for arg_child in arg.value.children:
                arg_str = str((arg_child, arg.arg_hash))
                expected_nodes.add(arg_str)
                expected_edges.add((arg_str, call_node_str))

        for result in values:
            result_str = str((result, [call_node]))

            expected_nodes.add(result_str)
            expected_edges.add((call_node_str, result_str))

        expected_nodes.add(call_node_str)

    dot_file = File(f"{exec_id}.dot")
    graph = pgv.AGraph(dot_file.read())

    expected_nodes.update(graph.nodes())
    assert len(expected_nodes) == 10

    produced_edges = graph.edges()
    assert len(produced_edges) == 9
    expected_edges.update(produced_edges)
    assert len(expected_edges) == 9

    file.remove()
    dot_file.remove()


def extract_edge_hashes(edges):
    edge_hashes = []
    for edge in edges:

        def extract_hash(node):
            return node.partition("=")[2].partition(",")[0].strip("'")

        hashed_edge = (extract_hash(str(edge[0])), extract_hash(str(edge[1])))

        if edge.attr["color"] == redun.visualize.SUBVALUE_EDGE_COLOR:
            hashed_edge = (hashed_edge[0], hashed_edge[1], True)

        edge_hashes.append(hashed_edge)

    return edge_hashes


def extract_subgraph_labels(graph):
    labels = [graph.graph_attr["label"]]
    for subgraph in graph.subgraphs():
        labels.extend(extract_subgraph_labels(subgraph))
    return labels


def test_value_viz():
    file_contents = """
from redun import task, File

@task()
def task3(x):
    return [x["first"], x["second"]]

@task()
def task2(x):
    return task3(x) + x["list"]

@task()
def task1(x):
    return task2(x)

@task()
def task4(x):
    return (x[0], x[1])

@task()
def main(x=[File("hi"), File("bye")]):
    z = task4(x)
    y = task1({"list": x, "first": z[0], "second": z[1]})
    return y[0]
"""

    argv = [
        "redun",
        "run",
        "workflow.py",
        "main",
    ]
    client, exec_id, file = run_workflow(argv, file_contents)
    client.execute(argv)

    query = CallGraphQuery(client.scheduler.backend.session)
    query = query.filter_types({"Execution"}).filter_execution_ids([exec_id])
    exec = query.one()
    value_id = exec.call_node.value.value_hash

    # Testing value visualization WITHOUT wrapping routing calls
    viz_argv = [
        "redun",
        "viz",
        value_id,
        "--format=dot",
    ]

    client.execute(viz_argv)

    expected_edge_hashes = {
        ("489d121b", "95183db3"),
        ("59e143c8", "489d121b"),
        ("59e143c8", "259b3e16"),
        ("59e143c8", "a7e1236b"),
        ("9338943f", "59e143c8", True),
        ("0094c848", "9338943f"),
        ("1cdefdf8", "0094c848"),
        ("1cdefdf8", "6b8f3a12"),
    }

    dot_file = File(f"{value_id}.dot")
    graph = pgv.AGraph(dot_file.read())

    produced_nodes = graph.nodes()
    assert len(produced_nodes) == 9

    produced_edge_hashes = extract_edge_hashes(graph.edges())
    assert len(produced_edge_hashes) == 8
    expected_edge_hashes.update(produced_edge_hashes)
    assert len(expected_edge_hashes) == 9

    viz_argv = [
        "redun",
        "viz",
        value_id,
        "--wrap-calls",
        "--format=dot",
    ]

    client.execute(viz_argv)

    expected_edge_hashes = {
        ("489d121b", "95183db3"),
        ("59e143c8", "489d121b"),
        ("9338943f", "59e143c8", True),
        ("0094c848", "9338943f"),
        ("1cdefdf8", "0094c848"),
    }

    graph = pgv.AGraph(dot_file.read())
    produced_edge_hashes = extract_edge_hashes(graph.edges())
    assert len(produced_edge_hashes) == 5
    expected_edge_hashes.update(produced_edge_hashes)
    assert len(expected_edge_hashes) == 5

    expected_labels = {("CallNode\nmain",), ("CallNode\ntask1", "CallNode\ntask2")}
    for subgraph in graph.subgraphs():
        labels = tuple(extract_subgraph_labels(subgraph))
        expected_labels.add(labels)
    assert len(expected_labels) == 2

    file.remove()
    dot_file.remove()
