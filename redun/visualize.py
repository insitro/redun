import re
from typing import Any, Dict, List, Tuple

import pygraphviz as pgv
from pygraphviz import AGraph

from redun.backends.db import CallNode, Execution, Job, RedunBackendDb, Value
from redun.backends.db.dataflow import ArgumentValue, CallNodeValue, walk_dataflow
from redun.backends.db.query import CallGraphQuery
from redun.scheduler import ErrorValue, Scheduler

JOB_COLOR = "#8FE0AC"
VALUE_COLOR = "#FF8484"
CALL_NODE_COLOR = "#B5D3E7"
SUBVALUE_EDGE_COLOR = "#9E9E9E"
DEFAULT_COLOR = ""
MAX_LABEL_LEN = 100
HALF_LABEL_LEN = MAX_LABEL_LEN // 2
SEPARATOR = "\n...\n"


def truncate(string: str) -> str:
    """
    Truncates a node label.
    """
    return string[:HALF_LABEL_LEN] + SEPARATOR + string[-HALF_LABEL_LEN:]


def clean_repr(value: Value) -> str:
    """
    Produces a representation of a value without the hash=<hash-id> section.
    """
    hash_regex = ", hash=.+?(?=\\))"
    return re.sub(hash_regex, "", repr(value.value_parsed))


def populate_valid_nodes(scheduler: Scheduler, exec_id: str, props: Dict[str, Any]) -> None:
    """
    Collects all the CallNodes associated with the provided execution id into a property.

    Used to validate CallNodes when accessed via parent-child relationships, which are not
    necessarily internal to an execution.
    """
    assert isinstance(scheduler.backend, RedunBackendDb)

    query = CallGraphQuery(scheduler.backend.session)
    call_nodes_query = query.filter_execution_ids([exec_id]).filter_types({"CallNode"})

    valid_call_nodes = [call_node.call_hash for call_node in call_nodes_query.all()]
    props["valid_call_nodes"] = valid_call_nodes


def init_graph(direction: str) -> AGraph:
    """
    Initializes a graph with the desired attributes.
    """
    graph = pgv.AGraph(strict=False, directed=True)
    graph.node_attr.update(
        {
            "shape": "box",
            "style": "filled",
            "fontname": "helvetica",
        }
    )
    graph.graph_attr.update(
        {
            "clusterrank": "local",
            "rankdir": direction,
            "splines": "polyline",
            "ranksep": 2,
            "fontname": "helvetica",
        }
    )
    return graph


def add_value(
    value: Value, upstream: Any, graph: AGraph, props: Dict[str, Any]
) -> List[Tuple[Value, Any]]:
    """
    Adds a Value and its subvalues to the provided graph.

    To give different values that may have the same parsed representation a unique identifier
    in the graph, we package the value with its upstream lineage. The upstream argument will either
    be a list of CallNodes or the arg hash if the value is top level.
    """
    color = VALUE_COLOR if isinstance(value.value_parsed, ErrorValue) else DEFAULT_COLOR
    if not props["dataflow"] or not value.children:
        hash_str = f" {value.value_hash[:8]}" if props["hash"] else ""
        value_str = f"Value{hash_str}\n{clean_repr(value)}"
        if len(value_str) > MAX_LABEL_LEN and not props.get("no_truncation", False):
            value_str = truncate(value_str)
        graph.add_node((value, upstream), fillcolor=color, label=value_str)
        return [(value, upstream)]
    else:
        values = []
        # This should go down to the leaf subvalues, as desired
        for arg_child in value.children:
            hash_str = f" {arg_child.value_hash[:8]}" if props["hash"] else ""
            value_str = f"Value{hash_str}\n{clean_repr(arg_child)}"
            if len(value_str) > MAX_LABEL_LEN and not props.get("no_truncation", False):
                value_str = truncate(value_str)
            graph.add_node((arg_child, upstream), label=value_str)
            values.append((arg_child, upstream))
        return values


def add_call_node(call_node: CallNode, graph: AGraph, props: Dict[str, Any]) -> None:
    """
    Adds a CallNode to the provided graph.
    """
    hash_str = f" {call_node.call_hash[:8]}" if props["hash"] else ""
    call_node_str = f"CallNode{hash_str}\n{call_node.task.fullname}"
    label = props.get("job_str", "") + call_node_str
    graph.add_node(call_node, fillcolor=CALL_NODE_COLOR, label=label)


def add_job(job: Job, graph: AGraph, props: Dict[str, Any], is_root: bool) -> None:
    """
    Adds a Job and its linked CallNode to the provided graph.

    Various combinations of properties will yield strictly CallNodes, strictly Jobs, or both.
    """
    call_node = job.call_node
    hash = f" {job.id[:8]}" if props["hash"] else ""
    opt_task = "" if props["detail"] else f"<BR />{job.call_node.task.fullname}"
    root = "ROOT<BR />" if is_root else ""

    if props["jobs"]:
        msg = f"{root}Job{hash}{opt_task}"
        if job.cached:
            msg = f'{msg}<BR /><FONT POINT-SIZE="10">(Cached)</FONT>'
        graph.add_node(job, fillcolor=JOB_COLOR, name=repr(job), label=f"<{msg}>")
    elif props["hash"]:
        props["job_str"] = f"Job{hash}\n"

    if props["detail"]:
        viz_call_node(call_node, graph, props)
        if props["jobs"]:
            graph.add_edge(job, call_node)


def process_routing_nodes(
    graph: AGraph,
    routing_arg: Any,
    routing_call_nodes: List[CallNode],
    props: Dict[str, Any],
) -> None:
    """
    Process a consecutive run of routing calls as a part of visualizing Values.

    After collecting the routing CallNodes that move around the same Value, we render them
    as subgraphs or nodes in the graph, depending on the properties.
    """
    add_value(routing_arg[0], routing_arg[1], graph, props)
    # Process consecutive runs of routing calls from outer to innermost
    routing_call_nodes.reverse()
    prev_graph = graph
    cluster_count = 0

    final_call_node = routing_call_nodes.pop(-1)
    if isinstance(final_call_node, CallNode):
        add_call_node(final_call_node, graph, props)
    graph.add_edge(routing_arg, final_call_node, color=DEFAULT_COLOR)

    for routing_call_node in routing_call_nodes:
        hash_str = f" {routing_call_node.call_hash[:8]}" if props["hash"] else ""
        label = f"CallNode{hash_str}\n{routing_call_node.task.fullname}"
        if props["wrap_calls"]:
            prev_graph.add_subgraph(
                [routing_arg],
                rank="same",
                name=f"cluster_{repr(routing_arg)}_{cluster_count}",
                margin=10.0,
                label=label,
                bgcolor=f"{CALL_NODE_COLOR}50",
            )
            prev_graph = graph.get_subgraph(f"cluster_{repr(routing_arg)}_{cluster_count}")
            cluster_count += 1
        else:
            add_call_node(routing_call_node, graph, props)
            graph.add_edge(routing_arg, routing_call_node, color=DEFAULT_COLOR)


def viz_value(scheduler: Scheduler, value: Value, graph: AGraph, props: Dict[str, Any]) -> None:
    """
    Visualizes a Value.

    Values are visualized by their upstream lineage.
    """
    routing_call_nodes = []
    routing_arg = None

    assert isinstance(scheduler.backend, RedunBackendDb)

    # Get upstream dataflow for value.
    dataflow_edges = walk_dataflow(scheduler.backend, value)

    for edge in dataflow_edges:
        src, dest = edge
        is_routing_edge, is_subvalue_edge = False, False
        if isinstance(src, CallNode):
            add_call_node(src, graph, props)
            complete_src: Any = src
        else:
            if isinstance(src, Value):
                # Then the upstream has to be a CallNode
                upstream: Any = [dest]
            elif isinstance(src, ArgumentValue):
                upstream = src.argument.upstream
                if not upstream:
                    upstream = src.argument.arg_hash
                src = src.value
                if not dest:
                    continue
                if isinstance(dest, ArgumentValue):
                    is_routing_edge = True
                if isinstance(dest, CallNodeValue):
                    is_subvalue_edge = True
                    # Handling special case of 1-to-1 mapping from CallNodeValue to ArgumentValue
                    # where there is no sub-value derivation involved
                    if dest.value == src:
                        continue
            elif isinstance(src, CallNodeValue):
                src, upstream = src.value, [src.call_node]
            else:
                raise ValueError(f"Unexpected dataflow node: {src}")

            complete_src = (src, upstream)
            is_derived_and_routed = (
                is_subvalue_edge and routing_arg and routing_arg[0] == complete_src[0]
            )
            if is_routing_edge or is_derived_and_routed:
                # We want to use the last ArgumentValue generated in a run of routing calls
                # since it includes the most complete upstream information, so we remove the node
                # if it was pre-emptively added, and store the outgoing CallNode to
                # reincorporate later
                if graph.has_node(complete_src):
                    out_edges = graph.out_edges([complete_src])
                    assert len(out_edges) == 1
                    outgoing_call_node = out_edges[0][1]
                    graph.remove_node(complete_src)
                    routing_call_nodes.append(outgoing_call_node)
                routing_arg = complete_src
            if not is_routing_edge:
                add_value(src, upstream, graph, props)

        if dest:
            if isinstance(dest, CallNode):
                add_call_node(dest, graph, props)
                complete_dest: Any = dest
            elif isinstance(dest, ArgumentValue):
                if is_routing_edge:
                    dest = dest.argument.call_node
                    routing_call_nodes.append(dest)
                else:
                    upstream = dest.argument.upstream
                    if not upstream:
                        upstream = dest.argument.arg_hash
                    dest = dest.value
                    add_value(dest, upstream, graph, props)
                    complete_dest = (dest, upstream)
            elif isinstance(dest, CallNodeValue):
                dest, upstream = dest.value, [dest.call_node]
                add_value(dest, upstream, graph, props)
                complete_dest = (dest, upstream)

            if not is_routing_edge:
                if routing_call_nodes:
                    process_routing_nodes(graph, routing_arg, routing_call_nodes, props)
                    routing_call_nodes = []
                color = SUBVALUE_EDGE_COLOR if is_subvalue_edge else DEFAULT_COLOR
                graph.add_edge(complete_dest, complete_src, color=color)

    if routing_call_nodes:
        process_routing_nodes(graph, routing_arg, routing_call_nodes, props)


def viz_call_node(call_node: CallNode, graph: AGraph, props: Dict[str, Any]) -> None:
    """
    Visualizes a CallNode.

    CallNodes are visualized from parents to children. CallNodes are the anchors
    by which argument and result values are also visualized.
    """
    add_call_node(call_node, graph, props)
    subgraph_nodes: List[Any] = [call_node]

    # Draw edges to parent CallNodes.
    if props.get("call_node_parents", True):
        for parent_call_node in call_node.parents:
            if parent_call_node.call_hash in props["valid_call_nodes"]:
                graph.add_edge(parent_call_node, call_node)

    # Draw argument nodes.
    for arg in call_node.arguments:
        arg_val = arg.value

        upstream = None
        if not props["deduplicate"]:
            upstream = arg.upstream
            # Guarantee node uniqueness for top level origin values with no upstream
            # by using their arg_hash instead
            if not upstream:
                upstream = arg.arg_hash

        values = add_value(arg_val, upstream, graph, props)
        for value in values:
            graph.add_edge(value, call_node)
            subgraph_nodes.append(value)

    # Draw the result node.
    result = call_node.value
    upstream = None if props["deduplicate"] else [call_node]
    values = add_value(result, upstream, graph, props)
    for value in values:
        graph.add_edge(call_node, value)
        subgraph_nodes.append(value)

    cluster_name = f"cluster-{call_node.call_hash[:8]}"
    graph.add_subgraph(subgraph_nodes, rank="same", name=cluster_name, style="invis")


def viz_job(job: Job, graph: AGraph, props: Dict[str, Any], is_root: bool) -> None:
    """
    Recursively visualizes a Job and all its children Jobs.
    """
    if is_root:
        add_job(job, graph, props, is_root)
        if props["jobs"]:
            graph.add_subgraph([job], rank="same", name="cluster-jobs", style="invis")
    for child_job in job.child_jobs:
        add_job(child_job, graph, props, False)
        if props["jobs"]:
            graph.add_edge(job, child_job)
            graph.get_subgraph("cluster-jobs").add_node(child_job)

        viz_job(child_job, graph, props, False)


def viz_execution(
    scheduler: Scheduler, execution: Execution, graph: AGraph, props: Dict[str, Any]
) -> None:
    """
    Visualizes an Execution.

    Executions are visualized by their root level job.
    """
    props["call_node_parents"] = True
    populate_valid_nodes(scheduler, execution.id, props)
    viz_job(execution.job, graph, props, True)


def viz_record(scheduler: Scheduler, record: Any, props: Dict[str, Any]) -> None:
    """
    Visualizes a record, generating resulting dot or png files as configured in the properties.

    Supports visualizing Executions, Jobs, CallNodes, and Values.
    Does NOT support task visualization.
    """
    # Override invalid flag combinations
    if not props["jobs"] and not props["detail"]:
        props["detail"] = True

    graph = init_graph(props["direction"])
    props["call_node_parents"] = False

    if isinstance(record, Execution):
        viz_execution(scheduler, record, graph, props)
        output = record.id
    elif isinstance(record, Job):
        add_job(record, graph, props, True)
        output = record.id
    elif isinstance(record, CallNode):
        viz_call_node(record, graph, props)
        output = record.call_hash
    elif isinstance(record, Value):
        viz_value(scheduler, record, graph, props)
        output = record.value_hash
    else:
        # We don't support Task visualization.
        raise ValueError(f"Visualization of this record type ({type(record)}) is not supported.")

    graph.layout(prog="dot")

    if props.get("output"):
        output = props["output"]

    format = props.get("format")
    if format == "png":
        graph.draw(f"{output}.png")
    elif format == "dot":
        graph.write(f"{output}.dot")
    elif format:
        raise ValueError("This visualization format is not supported.")
    else:
        graph.draw(f"{output}.png")
        graph.write(f"{output}.dot")
