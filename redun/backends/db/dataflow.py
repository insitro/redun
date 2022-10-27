"""
Dataflow visualization.

An upstream dataflow visualization explains the derivation of a value. Take
for example this dataflow visualization of the derivation of a VCF file
from a bioinformatic analysis:

```
value = File(path=sample4.vcf, hash=********)

value <-- <********> call_variants(bam, ref_genome)
  bam        = <********> File(path=sample4.bam, hash=********)
  ref_genome = <********> File(path=ref_genome, hash=********)

bam <-- argument of <********> call_variants_all(bams, ref_genome)
    <-- <********> align_reads_all(fastqs, ref_genome)
    <-- <********> align_reads(fastq, ref_genome)
  fastq      = <********> File(path=sample4.fastq, hash=********)
  ref_genome = <********> File(path=ref_genome, hash=********)

fastq <-- argument of <********> align_reads_all(fastqs, ref_genome)
      <-- argument of <********> main(fastqs, ref_genome)
      <-- origin

ref_genome <-- argument of <********> align_reads_all(fastqs, ref_genome)
           <-- argument of <********> main(fastqs, ref_genome)
           <-- origin
```

Hash values are indicated by * above.  For reference, here is what the
workflow might have been:

```
    @task()
    def align_reads(fastq: File, ref_genome: File) -> File:
        reads = cast(str, fastq.read())
        ref = cast(str, ref_genome.read())
        bam = File(fastq.path.replace("fastq", "bam"))
        bam.write("align({}, {})".format(reads, ref))
        return bam

    @task()
    def call_variants(bam: File, ref_genome: File) -> File:
        align = cast(str, bam.read())
        ref = cast(str, ref_genome.read())
        vcf = File(bam.path.replace("bam", "vcf"))
        vcf.write("calls({}, {})".format(align, ref))
        return vcf

    @task()
    def align_reads_all(fastqs: List[File], ref_genome: File):
        bams = [align_reads(fastq, ref_genome) for fastq in fastqs]
        return bams

    @task()
    def call_variants_all(bams: List[File], ref_genome: File):
        vcfs = [call_variants(bam, ref_genome) for bam in bams]
        return vcfs

    @task()
    def main(fastqs: List[File], ref_genome: File):
        bams = align_reads_all(fastqs, ref_genome)
        vcfs = call_variants_all(bams, ref_genome)
        return vcfs
```

A dataflow visualization consists of a series of paragraphs called
"dataflow sections" that describe how one of the values is derived. Here
is the section for the `bam` value:

```
bam <-- argument of <********> call_variants_all(bams, ref_genome)
    <-- <********> align_reads_all(fastqs, ref_genome)
    <-- <********> align_reads(fastq, ref_genome_2)
  fastq      = <********> File(path=sample4.fastq, hash=********)
  ref_genome = <********> File(path=ref_genome, hash=********)
```

A section is made of three clauses: assignment, routing, and arguments.

The assignment clause indicates which CallNode produced this value:

```
bam <-- argument of <********> call_variants_all(bams, ref_genome)
```

Routing clauses, if present, describe a series of additional CallNodes
that "route" the value by passing via arguments from parent CallNode to child
CallNode, or by results from child CallNode to parent CallNode.

```
    <-- result of <********> align_reads_all(fastqs, ref_genome)
    <-- <********> align_reads(fastq, ref_genome_2)
```

Argument clauses define the value for each argument in the final CallNode.

```
  fastq      = <********> File(path=sample4.fastq, hash=********)
  ref_genome = <********> File(path=ref_genome, hash=********)
```

To build this visualization, the following strategy is used:
- Given a starting value (e.g. a VCF file in the example above), walk the
  CallGraph backwards (i.e. upstream) to determine relevant nodes. These are
  call DataflowNodes, which are connected by DataflowEdges.
- DataflowEdges are then grouped into sections.
- Each section is then reorganized into a DataflowSectionDOM. A DataflowDOM
  is the collection of DataflowSectionDOMs. The DOM(document object model) is
  an intermediate representation that can be rendered in multiple ways.
- Once a DataflowDOM is created, it can either be rendered into a textual
  format, or serialized into JSON for the web frontend.
"""

import ast
import re
from collections import defaultdict
from enum import Enum
from itertools import chain
from textwrap import dedent
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from redun.backends.db import Argument, CallNode, RedunBackendDb, Task, Value
from redun.utils import assert_never, trim_string

T = TypeVar("T")

REDUN_INTERNAL_TASKS = {
    "redun.postprocess_script",
    "redun.script",
}


def iter_unique(items: Iterable[T], key: Callable[[T], Any] = lambda x: x) -> Iterator[T]:
    """
    Iterate through unique items.
    """
    seen: Set[T] = set()
    for item in items:
        item_key = key(item)
        if item_key not in seen:
            yield item
            seen.add(item_key)


class ArgumentValue(NamedTuple):
    """
    A DataflowNode used for tracing one subvalue in an argument.
    """

    argument: Argument
    value: Value


class CallNodeValue(NamedTuple):
    """
    A DataflowNode used for tracing one subvalue of a CallNode result.
    """

    call_node: CallNode
    value: Value


# There are several kinds of DataflowNodes.
DataflowNode = Union[ArgumentValue, CallNodeValue, CallNode, Value]


class DataflowEdge(NamedTuple):
    """
    An edge in a Dataflow graph.
    """

    src: DataflowNode
    dest: Optional[DataflowNode]


# A grouping of DataflowEdges that are displayed as one "paragraph".
DataflowSection = List[DataflowEdge]


class DataflowSectionKind(Enum):
    """
    Each dataflow section describes either a task call or a data manipulation.
    """

    CALL = "call"
    DATA = "data"


class DataflowAssign(NamedTuple):
    """
    The assignment clause in a Dataflow DOM.
    """

    var_name: str
    prefix: str
    node_display: str
    node: Optional[DataflowNode]


class DataflowRouting(NamedTuple):
    """
    A routing clause in a Dataflow DOM.
    """

    prefix: str
    node_display: str
    node: Optional[DataflowNode]


class DataflowArg(NamedTuple):
    """
    An argument clause in a Dataflow DOM.
    """

    var_name: str
    value: Value


class DataflowSectionDOM(NamedTuple):
    """
    A section in Dataflow DOM.
    """

    assign: DataflowAssign
    routing: List[DataflowRouting]
    args: List[DataflowArg]


# The top-level Dataflow DOM.
DataflowDOM = Iterable[DataflowSectionDOM]


def get_task_args(task: Task) -> List[str]:
    """
    Returns list of argument names of a Task. Raises a SyntaxError if the task source code is not
    properly formatted.
    """
    # Since we don't currently record the name of positional arguments,
    # we have to infer them from the source code.
    code = ast.parse(dedent(task.source))

    if not isinstance(code.body[0], ast.FunctionDef):
        raise SyntaxError("Source code is not a properly formatted function.")

    # Type ignore is needed since the AST lib does seem to use proper types
    # everywhere.
    return [arg.arg for arg in code.body[0].args.args]


def make_var_name(var_name_base: str, name2var: Dict[str, DataflowNode], suffix: int = 2) -> str:
    """
    Generate a new variable using a unique suffix (e.g. myvar_2).
    """
    # Strip numerical suffix.
    var_name_base = re.sub(r"_\d+$", "", var_name_base)

    if var_name_base not in name2var:
        # This variable name is already unique.
        return var_name_base

    # Search increase suffixes until we find a unique variable name.
    while True:
        new_var_name = var_name_base + "_" + str(suffix)
        if new_var_name not in name2var:
            return new_var_name
        suffix += 1


def get_default_arg_name(pos: int) -> str:
    """
    Generate the default name for argument.
    """
    return f"arg{pos}"


class DataflowVars:
    """
    Manages variable names for nodes in a dataflow.
    """

    def __init__(self):
        self.var2name: Dict[DataflowNode, str] = {}
        self.name2var: Dict[str, DataflowNode] = {}
        self.task2args: Dict[Task, List[str]] = {}

    def __getitem__(self, node: DataflowNode) -> str:
        """
        Get a variable name for a DataflowNode.
        """
        return self.var2name[node]

    def __setitem__(self, node: DataflowNode, var_name: str) -> str:
        """
        Set a new variable name for a DataflowNode.
        """
        self.var2name[node] = var_name
        self.name2var[var_name] = node
        return var_name

    def __contains__(self, node: DataflowNode) -> bool:
        """
        Returns True if node has a variable name.
        """
        return node in self.var2name

    def get_task_args(self, task: Task) -> List[str]:
        """
        Returns the parameter names of a Task.
        """
        args = self.task2args.get(task)
        if not args:
            # Cache task arg names.
            # TODO: Properly handle variadic args.
            try:
                args = self.task2args[task] = get_task_args(task)
            except SyntaxError:
                # The argument names cannot be properly parsed.
                return []
        return args

    def new_var_name(
        self, node: DataflowNode, base_var_name: Optional[str] = None
    ) -> Tuple[str, Optional[str]]:
        """
        Get or create a new variable name for a DataflowNode.
        """
        var_name = self.var2name.get(node)
        if var_name:
            # Node is already named.
            return var_name, None

        if not base_var_name:
            # Autogenerate base var name from ArgumentValue.
            assert isinstance(node, ArgumentValue)
            argument, value = node

            # Determine new variable name.
            if argument.arg_key:
                base_var_name = argument.arg_key
            else:
                arg_names = self.get_task_args(argument.call_node.task)
                if not arg_names:
                    # Use default argument names.
                    base_var_name = get_default_arg_name(argument.arg_position)
                else:
                    base_var_name = arg_names[argument.arg_position]

        # Ensure variable name is unique.
        var_name = make_var_name(base_var_name, self.name2var)
        self[node] = var_name
        return var_name, base_var_name


def walk_dataflow_value(backend: RedunBackendDb, value: Value) -> Iterator[DataflowEdge]:
    """
    Iterates through the edges in the upstream dataflow graph of a Value.
    """

    # Find upstream CallNodes.
    # A value can be produced by many CallNodes and it can be a subvalue of
    # a value produced from many CallNodes.
    call_nodes = set(value.results) | {
        call_node for parent in value.parents for call_node in parent.results
    }
    call_nodes = {call_node for call_node in call_nodes if not is_internal_task(call_node.task)}

    def walk_parents(node: CallNode, seen: set) -> Iterator[CallNode]:
        for parent in node.parents:
            if parent not in seen:
                yield parent
                seen.add(parent)
                yield from walk_parents(parent, seen)

    # Determine which CallNodes are just routing CallNodes.
    # A routing CallNode is an upstream CallNode that is also an ancestor
    # of another upstream CallNode.
    seen: Set[CallNode] = set()
    routing_call_nodes = set()
    for call_node in call_nodes:
        for ancestor in walk_parents(call_node, seen):
            if ancestor in call_nodes:
                routing_call_nodes.add(ancestor)
                break

    # Determine originating CallNodes (upstream and non-routing).
    originating_call_nodes = call_nodes - routing_call_nodes

    # Prefer the most recent CallNodes.
    start_time_call_nodes = [
        (job.start_time, call_node)
        for call_node in originating_call_nodes
        for job in call_node.jobs
    ]
    max_start_time = max((start_time for start_time, _ in start_time_call_nodes), default=None)
    upstream_call_node = next(
        (
            call_node
            for start_time, call_node in start_time_call_nodes
            if start_time == max_start_time
        ),
        None,
    )

    # Emit Value-CallNode edges in dataflow.
    if upstream_call_node:
        yield DataflowEdge(value, upstream_call_node)
    else:
        yield DataflowEdge(value, None)


def get_callnode_arguments(call_node: CallNode) -> List[Argument]:
    """
    Returns a CallNode's arguments in sorted order.
    """
    pos_args = []
    kw_args = []
    for arg in call_node.arguments:
        if arg.arg_position is not None:
            pos_args.append(arg)
        else:
            kw_args.append(arg)
    pos_args.sort(key=lambda arg: arg.arg_position)
    kw_args.sort(key=lambda arg: arg.arg_key)
    return pos_args + kw_args


def walk_dataflow_callnode(backend: RedunBackendDb, call_node: CallNode) -> Iterator[DataflowEdge]:
    """
    Iterates through the upstream Arguments of a CallNode.
    """
    # Emit CallNode-ArgumentValue edges in dataflow.
    # Reversing the arguments will lead to the traversal to be in original
    # argument order, which is nicer for display.
    arguments = get_callnode_arguments(call_node)
    if arguments:
        for argument in arguments:
            yield DataflowEdge(call_node, ArgumentValue(argument, argument.value))
    else:
        # There are no arguments, this is an origin of the dataflow.
        yield DataflowEdge(call_node, None)


def is_internal_task(task: Task) -> bool:
    """
    Returns True if task is an internal redun task.

    We skip such tasks in the dataflow to avoid clutter.
    """
    return task.fullname in REDUN_INTERNAL_TASKS


def walk_dataflow_callnode_value(
    backend: RedunBackendDb, call_node_value: CallNodeValue
) -> Iterator[DataflowEdge]:
    """
    Iterates through the upstream dataflow edges of a CallNodeValue.

    The edges either go deeper to the child CallNodes or stay with this CallNode.
    """
    call_node, value = call_node_value

    def is_subvalue(query_value: Value, target_value: Value) -> bool:
        value_hashes = chain(
            [target_value.value_hash], (child.value_hash for child in target_value.children)
        )
        return query_value.value_hash in value_hashes

    # Prefer the flow from children of call_node over call_node.
    child_matches = [
        child_node
        for child_node in call_node.children
        if not is_internal_task(child_node.task) and is_subvalue(value, child_node.value)
    ]
    if len(child_matches) == 1:
        # There is one obvious child CallNode that produced this value.
        # Follow the dataflow through this child CallNode.
        [child_node] = child_matches
        yield DataflowEdge(call_node_value, CallNodeValue(child_node, value))
    else:
        # Otherwise, we follow the dataflow for the whole CallNode.
        yield DataflowEdge(call_node_value, call_node)


def walk_dataflow_argument_value(
    backend: RedunBackendDb, argument_value: ArgumentValue
) -> Iterator[DataflowEdge]:
    """
    Iterates through the upstream dataflow edges of an ArgumentValue.

    Edge types:
    - ArgumentValue <-- CallNodeValue:
      - Value came from the result of a CallNode.
    - ArgumentValue <-- ArgumentValue
      - Value came from argument of parent CallNode, i.e. argument-to-argument routing.
    - ArgumentValue <-- Origin
      - Value came directly from user or task.
    """
    argument, value = argument_value
    is_terminal = True

    # Determine the most recent common parent CallNode of all the upstream CallNodes.
    call_node_parents = [set(call_node.parents) for call_node in argument.upstream]
    call_node_parents.append(set(argument.call_node.parents))
    context_call_node = next(
        iter(
            sorted(
                set.intersection(*call_node_parents),
                key=lambda call_node: call_node.timestamp,
                reverse=True,
            )
        ),
        None,
    )

    # Process upstream CallNodes in a consistent order (call_order).
    if context_call_node:
        # Use reverse order during emission to get correct order during display.
        upstream_order = sorted(
            [
                (call_node, edge.call_order)
                for call_node in argument.upstream
                for edge in call_node.parent_edges
                if edge.parent_node == context_call_node
            ],
            key=lambda pair: pair[1],
            reverse=True,
        )
        upstream = [call_node for call_node, _ in upstream_order]
    else:
        # Fallback if we can't determine context call node.
        upstream = argument.upstream

    # Emit upstream sibling CallNodes.
    for call_node in iter_unique(upstream):
        # Order subvalues for consistency.
        subvalues = sorted(call_node.value.children, key=lambda child: child.value_hash)
        result_values = iter_unique(chain([call_node.value], subvalues))
        match = False
        for result_value in result_values:
            if value.value_hash == result_value.value_hash:
                is_terminal = False
                match = True
                yield DataflowEdge(
                    argument_value,
                    CallNodeValue(call_node, result_value),
                )

        if not match:
            # Default to emitting the whole result of the CallNode.
            is_terminal = False
            yield DataflowEdge(
                argument_value,
                CallNodeValue(call_node, call_node.value),
            )

    # Emit upstream argument from parent CallNode.
    # Prefer most recent parent.
    parent_call_nodes = sorted(
        argument.call_node.parents, key=lambda parent: parent.timestamp, reverse=True
    )
    for parent_call_node in parent_call_nodes[:1]:
        for parent_argument in parent_call_node.arguments:
            parent_values = chain([parent_argument.value], parent_argument.value.children)
            for parent_value in parent_values:
                if value.value_hash == parent_value.value_hash:
                    is_terminal = False
                    yield DataflowEdge(
                        argument_value,
                        ArgumentValue(parent_argument, value),
                    )

    # Emit terminal origin value.
    if is_terminal:
        yield DataflowEdge(argument_value, None)


def walk_dataflow_node(backend: RedunBackendDb, node: DataflowNode) -> Iterator[DataflowEdge]:
    """
    Iterates through the upstream dataflow edges of a any DataflowNode.
    """
    if isinstance(node, Value):
        return walk_dataflow_value(backend, node)

    elif isinstance(node, CallNode):
        return walk_dataflow_callnode(backend, node)

    elif isinstance(node, ArgumentValue):
        return walk_dataflow_argument_value(backend, node)

    elif isinstance(node, CallNodeValue):
        return walk_dataflow_callnode_value(backend, node)

    else:
        assert_never(node)


def walk_dataflow(backend: RedunBackendDb, init_node: DataflowNode) -> Iterator[DataflowEdge]:
    """
    Iterate through all the upstream dataflow edges of a 'node' in the CallGraph.

    A 'node' can be a Value, CallNode, CallNodeValue, or an ArgumentValue.
    """
    # Perform depth-first traversal.
    queue: List[DataflowNode] = [init_node]
    seen: Set[DataflowNode] = set()

    while queue:
        node: DataflowNode = queue.pop()
        child_edges = list(walk_dataflow_node(backend, node))
        yield from child_edges

        # Reverse edges before pushing on to stack to maintain sibling order.
        for edge in reversed(child_edges):
            node2 = edge.dest
            if node2 is None:
                # Terminal node of type 'Origin'.
                continue

            # Determine whether dest node is unique and should be added to queue.
            if node2 not in seen:
                queue.append(node2)
                seen.add(node2)


def get_section_edge_type(edge: DataflowEdge) -> str:
    """
    Classifies a DataflowEdge.
    """
    src, dest = edge
    if (
        isinstance(src, ArgumentValue)
        and isinstance(dest, CallNodeValue)
        and src.value.value_hash != dest.value.value_hash
    ):
        return "data"

    elif isinstance(src, CallNode) and isinstance(dest, ArgumentValue):
        return "call_arg"

    elif isinstance(src, (Value, CallNodeValue)) and isinstance(dest, CallNode):
        return "call_result"

    elif isinstance(src, CallNodeValue) and isinstance(dest, CallNodeValue):
        return "call_result_routing"

    elif isinstance(src, ArgumentValue) and isinstance(dest, CallNodeValue):
        return "call_arg_result_routing"

    elif isinstance(src, ArgumentValue) and isinstance(dest, ArgumentValue):
        return "call_arg_routing"

    elif isinstance(src, CallNode) and dest is None:
        return "call_origin"

    elif isinstance(src, ArgumentValue) and dest is None:
        return "call_arg_origin"

    elif isinstance(src, Value) and dest is None:
        return "call_value_origin"

    else:
        raise AssertionError(f"Unknown edge type {edge}")


def toposort_edges(edges: Iterable[DataflowEdge]) -> Iterator[DataflowEdge]:
    """
    Topologically sort DataflowEdges in depth-first order.
    """

    # Compute indegree.
    indegrees: Dict[DataflowNode, int] = defaultdict(int)
    src2edge: Dict[DataflowNode, List[DataflowEdge]] = defaultdict(
        cast(Callable[[], List[DataflowEdge]], list)
    )
    for edge in edges:
        src2edge[edge.src].append(edge)

        # Ensure every node is present in indegrees, including roots.
        indegrees[edge.src]

        if edge.dest:
            indegrees[edge.dest] += 1

    # Initialize queue with roots.
    queue: List[DataflowNode] = [node for node, degree in indegrees.items() if degree == 0]

    while queue:
        node = queue.pop()
        yield from src2edge[node]

        # Reverse edges before pushing on stack in order to maintain sibling order.
        for edge in reversed(src2edge[node]):
            if edge.dest:
                indegrees[edge.dest] -= 1
                if indegrees[edge.dest] == 0:
                    # All parents have been visited, we can enqueue dest.
                    queue.append(edge.dest)


def rewrite_call_node_merges(edges: List[DataflowEdge]) -> List[DataflowEdge]:
    """
    Rewrites dataflow graphs to enforce one CallNodeValue per CallNode.

    This function identifies CallNode that have multilple parent CallNodeValues
    like this:

      ArgumentValue --> CallNodeValue --\
                                         V
                                         CallNode (merge_node)
                                         ^
      ArgumentValue --> CallNodeValue --/

    and rewrite them to unify the CallNodeValues like this:

      ArgumentValue --\
                       V
                       CallNodeValue --> CallNode
                       ^
      ArgumentValue --/
    """

    # Build graph dict.
    src2edges: Dict[Optional[DataflowNode], List[DataflowEdge]] = defaultdict(list)
    dest2edges = defaultdict(list)
    nodes = []
    for edge in edges:
        nodes.append(edge.src)
        src2edges[edge.src].append(edge)
        if edge.dest:
            dest2edges[edge.dest].append(edge)

    # Find merge nodes.
    merge_nodes = [
        node for node, edges in dest2edges.items() if isinstance(node, CallNode) and len(edges) > 1
    ]

    # Rewrite CallNode merge nodes.
    for merge_node in merge_nodes:
        # Find relevant edges and nodes.
        cv_cn_edges = dest2edges[merge_node]
        call_node_values = [edge.src for edge in cv_cn_edges]
        upstream_edges = [
            edge for call_node_value in call_node_values for edge in dest2edges[call_node_value]
        ]
        upstream_nodes = list(iter_unique(edge.src for edge in upstream_edges))
        old_edges = set(cv_cn_edges) | set(upstream_edges)

        # Create new unified CallNodeValue from call_node_values.
        unified_node = CallNodeValue(call_node=merge_node, value=merge_node.value)

        # Create new edges.
        new_edges = [
            DataflowEdge(
                src=upstream_node,
                dest=unified_node,
            )
            for upstream_node in upstream_nodes
        ] + [DataflowEdge(src=unified_node, dest=merge_node)]

        # Remove old edges from edges.
        edges2 = [edge for edge in edges if edge not in old_edges]

        # To keep edges in traversal order, insert new edges right before
        # the first appearance of the merge_node.
        insert_index = min(i for i, edge in enumerate(edges2) if edge.src == merge_node)
        edges = edges2[:insert_index] + new_edges + edges2[insert_index:]

    return edges


def iter_subsections(section: DataflowSection) -> Iterator[DataflowSection]:
    """
    Determines if a section should be broken down into small sections.

    In real life dataflows, there are some cases where the dataflow merges
    such that the structure is a DAG, not just a tree. These merges represent a
    value that was passed to two or more different tasks and then their outputs
    eventually combine again, either into a single Value like a list or as arguments
    into a common task. For example, `value` is a merge node in the upstream
    dataflow of `result`.

      value = task0()
      output1 = task1(a=value)
      output2 = task2(b=value)
      result = task3(c=output1, d=output2)

    The upstream dataflow graph of `result` is:

      Value(result)
       |
       V
      CallNode(task3) ---------------\
       |                             |
       V                             V
      ArgumentValue(task3, key=a) ArgumentValue(task3, key=b)
       |                             |
       V                             V
      CallNode(task1)             CallNode(task2)
       |                             |
       V                             V
      ArgumentValue(task1, key=c) ArgumentValue(task2, key=d)
       |                             |
       V                             |
      CallNodeValue(task0, value) <--/
       |
       V
      CallNode(task0)
       |
       V
      Origin

    The function `iter_dataflow_section()` will break this graph right after
    every the `CallNode --> ArgumentValue` edge, resulting in three sections.
    One of those sections will have a "merge node", `CallNode(task0)`:

      ArgumentValue(task1, key=c) ArgumentValue(task2, key=d)
       |                             |
       V                             |
      CallNodeValue(task0, value) <--/
       |
       V
      CallNode(task0)
       |
       V
      Origin

    This function will further break this section into subsections like this:

    Subsection 1:

      ArgumentValue(task1, key=c)
       |
       V
      CallNodeValue(task0, value)

    Subsection 2:

      ArgumentValue(task2, key=d)
       |
       V
      CallNodeValue(task0, value)

    Subsection 3:

      CallNodeValue(task0, value)
       |
       V
      CallNode(task0)
       |
       V
      Origin

    Ultimately these three subsections get rendered as:

      c <-- c_2

      d <-- c_2

      c_2 <-- task()
          <-- origin
    """

    # Build graph dict.
    src2edges: Dict[Optional[DataflowNode], List[DataflowEdge]] = defaultdict(list)
    dest2edges = defaultdict(list)
    nodes = []
    for edge in section:
        nodes.append(edge.src)
        src2edges[edge.src].append(edge)
        if edge.dest:
            dest2edges[edge.dest].append(edge)

    # Find roots. Maintain order of appearance in section.
    roots = [node for node in iter_unique(nodes) if len(dest2edges[node]) == 0]

    # Find merge nodes.
    merge_nodes = [node for node, edges in dest2edges.items() if len(edges) > 1]

    if not merge_nodes:
        # No merge nodes. Keep section as is.
        yield section
        return

    # Determine subsections.
    subsection: DataflowSection = []
    node: Optional[DataflowNode]
    for node in roots + merge_nodes:
        while True:
            next_edges = src2edges[node]
            if len(next_edges) != 1:
                # We have hit the end of the section.
                # There are either no more edges, or we hitting the arguments.
                subsection.extend(next_edges)
                yield subsection
                subsection = []
                break

            # Path should always be linear.
            [edge] = next_edges
            subsection.append(edge)

            if edge.dest not in merge_nodes:
                # Follow edge.
                node = edge.dest
            else:
                # We have hit the merge node, stop.
                yield subsection
                subsection = []
                break


def iter_dataflow_sections(
    dataflow_edges: Iterable[DataflowEdge],
) -> Iterator[Tuple[DataflowSectionKind, DataflowSection]]:
    """
    Yields dataflow sections from an iterable of dataflow edges.

    A dataflow section is a group of edges representing one 'paragraph' in a
    dataflow display.

      value <-- <1234abcd> call_node(arg1, arg2)
            <-- result of <2345abcd> call_node2(arg3, arg4)
        arg3 = <3456abcd> 'hello_world'
        arg4 = <4567abcd> File('foo.txt')
    """
    node2call_section: Dict[DataflowNode, List[Tuple[int, DataflowEdge]]] = {}
    node2data_section: Dict[DataflowNode, List[Tuple[int, DataflowEdge]]] = defaultdict(
        cast(Callable[[], List[Tuple[int, DataflowEdge]]], list)
    )

    new_vars: Set[ArgumentValue] = set()
    section: List[Tuple[int, DataflowEdge]]

    edges = toposort_edges(dataflow_edges)
    edge_list = rewrite_call_node_merges(list(edges))

    # Group dataflow edges into display sections.
    # Retain the appearance order of each edge so we can sort sections later.
    for i, edge in enumerate(edge_list):
        edge_type = get_section_edge_type(edge)

        if edge_type == "data":
            # Edge is a data section edge.
            node2data_section[edge.src].append((i, edge))
            continue

        if edge_type == "call_arg":
            # New variable.
            assert isinstance(edge.dest, ArgumentValue)
            new_vars.add(edge.dest)

        if edge.src in new_vars:
            # src is a new variable so we start a new section.
            section = [(i, edge)]

        else:
            # Get or create section associated with src and add this edge.
            if edge.src not in node2call_section:
                section = node2call_section[edge.src] = []
            else:
                section = node2call_section[edge.src]
            section.append((i, edge))

        # Get section associated with dest and union with src section.
        if edge.dest:
            if edge.dest not in node2call_section:
                # dest_section same as src.
                node2call_section[edge.dest] = section
            else:
                # Union dest_section with section.
                # Although this extend might have dups, I use iter_unique to clean it up.
                dest_section = node2call_section[edge.dest]
                dest_section.extend(section)
                node2call_section[edge.src] = dest_section

    # Get unique sections.
    call_sections = iter_unique(node2call_section.values(), key=id)
    data_sections = node2data_section.values()

    def get_order_section(
        int_edges: List[Tuple[int, DataflowEdge]]
    ) -> Tuple[int, List[DataflowEdge]]:
        """
        Returns a tuple of section appearance order and the section.

        The appearance order of a section is the maximum order of its edges.
        We also clean up any duplicate edges that may have been added to the section.
        """
        ints = []
        section = []
        for i, edge in int_edges:
            ints.append(i)
            section.append(edge)
        return (max(ints), list(iter_unique(section)))

    # Label each section with its type ("call" or "data") and determine section order.
    sections = [
        (DataflowSectionKind.CALL,) + get_order_section(int_edges) for int_edges in call_sections
    ]
    sections.extend(
        (DataflowSectionKind.DATA,) + get_order_section(int_edges) for int_edges in data_sections
    )

    # Sort sections.
    sections = sorted(sections, key=lambda row: row[1])

    # Yield sections.
    for kind, _, section2 in sections:
        if kind == DataflowSectionKind.CALL:
            # If there is path merging, then we will emit multiple sections.
            for subsection in iter_subsections(section2):
                yield (kind, subsection)
        else:
            yield (kind, section2)


def make_section_dom(
    section: DataflowSection,
    dataflow_vars: DataflowVars,
    new_varname: str = "value",
) -> DataflowSectionDOM:
    """
    Returns DOM for a dataflow section.
    """
    # Determine assign information from first edge in section.
    src, assign_node = section[0]
    if isinstance(src, Value):
        # Start new variable.
        assign_var_name = dataflow_vars[src] = new_varname

    elif isinstance(src, (ArgumentValue, CallNodeValue)):
        # Value should be named already.
        assign_var_name = dataflow_vars[src]

    else:
        assert not isinstance(src, CallNode)
        assert_never(src)

    routing_defs: List[Optional[DataflowNode]] = []
    arg_defs: List[Tuple[str, Value]] = []
    renames: Dict[str, str] = {}

    # Compute whether this section ends with a variable.
    is_var2var = isinstance(assign_node, (ArgumentValue, CallNodeValue))
    last_routing_node = None

    # Process remaining edges.
    for src, dest in section[1:]:
        if isinstance(src, CallNode) and isinstance(dest, ArgumentValue):
            # Argument definition edge.
            var_name, base_var_name = dataflow_vars.new_var_name(dest)
            if base_var_name:
                renames[base_var_name] = var_name
            arg_defs.append((var_name, dest.value))

        else:
            # Routing edge.
            last_routing_node = dest
            if isinstance(dest, CallNode):
                is_var2var = False

            # Skip unnecessary routing edge, ignore.
            if not (isinstance(src, CallNodeValue) and isinstance(dest, CallNode)):
                routing_defs.append(dest)

    # The last routing def or assign (if there are no routing defs) needs
    # to rename its variables to be unique.
    last_line = len(routing_defs)

    # Create assign clause.
    if is_var2var and not routing_defs:
        # Assignment is a var2var.
        assert assign_node
        # Name the last node after the first node, if it doesn't have a name yet.
        base_name = dataflow_vars[section[0].src]
        var_name, _ = dataflow_vars.new_var_name(assign_node, base_name)
        dom_assign = DataflowAssign(assign_var_name, "", var_name, assign_node)
    else:
        prefix, node_display = display_node(assign_node, renames if last_line == 0 else {})
        dom_assign = DataflowAssign(assign_var_name, prefix, node_display, assign_node)

    # Create routing clauses.
    dom_routing: List[DataflowRouting] = []
    for i, dest in enumerate(routing_defs, 1):
        prefix, node_display = display_node(dest, renames if i == last_line else {})
        dom_routing.append(DataflowRouting(prefix, node_display, dest))

    # If last routing node is a Value, as opposed to a CallNode, then this was a
    # merge node and we should end with a new variable.
    if is_var2var and isinstance(last_routing_node, (ArgumentValue, CallNodeValue)):
        # This is a merge node, give it a variable name.
        assert not arg_defs

        # Name the last node after the first node, if it doesn't have a name yet.
        base_name = dataflow_vars[section[0].src]
        var_name, _ = dataflow_vars.new_var_name(last_routing_node, base_name)
        dom_routing.append(DataflowRouting("", var_name, last_routing_node))

    # Create argument definitions.
    dom_args: List[DataflowArg] = [DataflowArg(var_name, var) for var_name, var in arg_defs]

    return DataflowSectionDOM(dom_assign, dom_routing, dom_args)


def make_data_section_dom(
    section: DataflowSection,
    dataflow_vars: DataflowVars,
    new_varname: str = "value",
) -> DataflowSectionDOM:
    """
    Returns a DOM for a data section.

    A data section describes how one value was constructed from several subvalues.
    For example this dataflow section:

      parent_value <-- derives from
        subvalue1 = File(path='file1')
        subvalue2 = File(path='file2')

    corresponds to this kind of code:

      subvalue1 = task1()
      subvalue2 = task2()
      parent_value = [subvalue1, subvalue2]
      task3(parent_value)
    """
    downstream_nodes: Set[DataflowNode] = set()
    upstream_nodes: List[CallNodeValue] = []

    for edge in section:
        assert isinstance(edge.dest, CallNodeValue)
        downstream_nodes.add(edge.src)
        upstream_nodes.append(edge.dest)

    # Determine assign clause.
    # We only support one downstream node currently.
    [downstream_node] = downstream_nodes
    assign_var_name = dataflow_vars[downstream_node]

    # Determine arg clauses.
    args = []
    for upstream_node in iter_unique(upstream_nodes):
        call_node, value = upstream_node

        # Ensure variable name is unique.
        base_var_name = upstream_node.call_node.task.name + "_result"
        new_var_name, _ = dataflow_vars.new_var_name(upstream_node, base_var_name)
        args.append(DataflowArg(new_var_name, value))

    return DataflowSectionDOM(
        assign=DataflowAssign(
            var_name=assign_var_name, prefix="", node_display="derives from", node=None
        ),
        routing=[],
        args=args,
    )


def make_dataflow_dom(
    dataflow_edges: Iterable[DataflowEdge], new_varname: str = "value"
) -> Iterable[DataflowSectionDOM]:
    """
    Yields dataflow section DOMs from an iterable of dataflow edges.

    It also performs variable renaming to give every value a unique variable name.
    """
    dataflow_vars = DataflowVars()

    for kind, section in iter_dataflow_sections(dataflow_edges):
        if kind == DataflowSectionKind.CALL:
            yield make_section_dom(section, dataflow_vars, new_varname=new_varname)

        elif kind == DataflowSectionKind.DATA:
            yield make_data_section_dom(section, dataflow_vars, new_varname=new_varname)

        else:
            raise NotImplementedError(f"Unknown kind '{kind}'.")


def get_dataflow_call_node(node: Optional[DataflowNode]) -> Optional[CallNode]:
    """
    Returns the CallNode for a DataflowNode.
    """
    if isinstance(node, CallNode):
        return node

    elif isinstance(node, ArgumentValue):
        return node.argument.call_node

    elif isinstance(node, CallNodeValue):
        return node.call_node

    elif node is None:
        return None

    else:
        assert_never(node)


def get_node_hash(node: Optional[DataflowNode]) -> Optional[str]:
    """
    Formats hash for a DataflowNode.
    """
    if isinstance(node, Value):
        return node.value_hash

    call_node = get_dataflow_call_node(node)
    if call_node:
        return call_node.call_hash

    return None


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
    return trim_string(repr(value.preview))


def display_hash(node: Optional[DataflowNode]) -> str:
    """
    Formats hash for a DataflowNode.
    """
    node_hash = get_node_hash(node)
    if node_hash:
        return "<{}> ".format(node_hash[:8])
    else:
        return ""


def display_section(dom: DataflowSectionDOM) -> Iterator[str]:
    """
    Yields lines for displaying a dataflow section DOM.
    """

    # Display assign line.
    assign = dom.assign
    yield "{var_name} <-- {prefix}{value_hash}{value}".format(
        var_name=assign.var_name,
        prefix=assign.prefix + " " if assign.prefix else "",
        value_hash=display_hash(assign.node),
        value=assign.node_display,
    )

    indent = len(assign.var_name) + 1

    # Display routing.
    for routing_def in dom.routing:
        yield "{indent}<-- {prefix}{node_hash}{node}".format(
            indent=" " * indent,
            prefix=routing_def.prefix + " " if routing_def.prefix else "",
            node_hash=display_hash(routing_def.node),
            node=routing_def.node_display,
        )

    # Display argument definitions.
    if dom.args:
        max_var_len = max(len(arg.var_name) for arg in dom.args)
        for arg in dom.args:
            yield "  {var_name}{padding} = <{value_hash}> {var}".format(
                var_name=arg.var_name,
                padding=" " * (max_var_len - len(arg.var_name)),
                value_hash=arg.value.value_hash[:8],
                var=display_value(arg.value),
            )


def display_dataflow(dom: DataflowDOM) -> Iterator[str]:
    """
    Yields for lines displaying a dataflow DOM.
    """
    for dom_section in dom:
        yield from display_section(dom_section)
        yield ""


def serialize_section(dom: DataflowSectionDOM) -> dict:
    """
    Serialize a DataflowSectionDOM to JSON.
    """
    # Serialize assignment.
    assign_hash: Optional[str]
    if isinstance(dom.assign.node, Value):
        assign_hash = dom.assign.node.value_hash
        assign_job_id = None
        assign_execution_id = None
    elif dom.assign.node:
        call_node = get_dataflow_call_node(dom.assign.node)
        assert call_node
        assign_hash = call_node.call_hash
        # Get latest job of call_node.
        assign_job = sorted(call_node.jobs, key=lambda job: job.start_time, reverse=True)[0]
        assign_job_id = assign_job.id
        assign_execution_id = assign_job.execution.id
    else:
        assign_hash = None
        assign_job_id = None
        assign_execution_id = None

    # Serialize routing.
    routing = []
    for routing_def in dom.routing:
        call_node = get_dataflow_call_node(routing_def.node)
        if call_node:
            call_hash: Optional[str] = call_node.call_hash
            # Get latest job of call_node.
            job = sorted(call_node.jobs, key=lambda job: job.start_time, reverse=True)[0]
            job_id: Optional[str] = job.id
            execution_id: Optional[str] = job.execution.id
        else:
            call_hash = None
            job_id = None
            execution_id = None

        routing.append(
            {
                "prefix": routing_def.prefix,
                "hash": call_hash,
                "display": routing_def.node_display,
                "job_id": job_id,
                "execution_id": execution_id,
            }
        )

    # Serialize arguments.
    args = [
        {
            "var_name": arg.var_name,
            "hash": arg.value.value_hash,
            "value_display": display_value(arg.value),
        }
        for arg in dom.args
    ]

    return {
        "assign": {
            "var_name": dom.assign.var_name,
            "prefix": dom.assign.prefix,
            "display": dom.assign.node_display,
            "hash": assign_hash,
            "job_id": assign_job_id,
            "execution_id": assign_execution_id,
        },
        "routing": routing,
        "args": args,
    }


def serialize_dataflow(dom: DataflowDOM) -> Iterator[dict]:
    """
    Serialize DataflowDOM to JSON.
    """
    for dom_section in dom:
        yield serialize_section(dom_section)
