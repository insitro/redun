from typing import Dict, List, cast

from redun import File, Scheduler, task
from redun.backends.db import CallNode, Execution, RedunBackendDb, Value
from redun.backends.db.dataflow import (
    ArgumentValue,
    CallNodeValue,
    Task,
    display_dataflow,
    make_dataflow_dom,
    serialize_dataflow,
    walk_dataflow,
    walk_dataflow_argument_value,
    walk_dataflow_callnode,
    walk_dataflow_callnode_value,
    walk_dataflow_value,
)
from redun.tests.utils import Match, assert_match_text, use_tempdir


@use_tempdir
def test_dataflow_crawl(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Manually crawl through dataflow to ensure we can extract each kind of edge.
    """
    assert backend.session

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

    # Create dataset.
    fastqs = []
    for i in range(5):
        fastq = File("sample{}.fastq".format(i))
        fastq.write("reads{}".format(i))
        fastqs.append(fastq)

    ref_genome = File("ref_genome")
    ref_genome.write("genome")

    # Run workflow.
    scheduler.run(main(fastqs, ref_genome))

    # Quick check workflow works as intended.
    assert File("sample4.vcf").read() == "calls(align(reads4, genome), genome)"

    vcf = File("sample4.vcf")
    value = backend.session.query(Value).filter(Value.value_hash == vcf.get_hash()).one()

    # Ensure we can walk to originating call nodes.
    edges = list(walk_dataflow_value(backend, value))
    assert [call_node.task_name for _, call_node in edges] == ["call_variants"]  # type: ignore

    # Ensure we can walk to arguments.
    [(_, call_node)] = edges
    assert isinstance(call_node, CallNode)
    edges2 = list(walk_dataflow_callnode(backend, call_node))

    # Ensure we can walk to upstream of arguments.
    [(_, bam_arg_val), (_, ref_arg_val)] = edges2
    assert isinstance(ref_arg_val, ArgumentValue)
    assert isinstance(bam_arg_val, ArgumentValue)
    edges3 = list(walk_dataflow_argument_value(backend, bam_arg_val))
    edges4 = list(walk_dataflow_argument_value(backend, ref_arg_val))

    # Ensure we can walk upstream of arguments again.
    [(_, bam_arg_val2)] = edges3
    [(_, ref_arg_val2)] = edges4
    assert isinstance(bam_arg_val2, ArgumentValue)
    edges5 = list(walk_dataflow_argument_value(backend, bam_arg_val2))
    # edges6 = list(walk_dataflow_argument_value(scheduler.backend, ref_arg_val2))

    # Walk bam back to specific align_reads() call node.
    [(_, bam_call_node_val)] = edges5
    assert isinstance(bam_call_node_val, CallNodeValue)
    edges7 = list(walk_dataflow_callnode_value(backend, bam_call_node_val))
    assert isinstance(edges7[0][1], CallNodeValue)
    assert edges7[0][1][1].file.path == "sample4.bam"

    # Walk bam back to fastq.
    [(_, bam_call_node_val2)] = edges7
    assert isinstance(bam_call_node_val2, CallNodeValue)
    edges8 = list(walk_dataflow_callnode_value(backend, bam_call_node_val2))

    [(_, align_node)] = edges8
    assert isinstance(align_node, CallNode)
    edges9 = list(walk_dataflow_callnode(backend, align_node))
    [fastq_edge, ref_edge] = edges9

    assert isinstance(fastq_edge[1], tuple)
    assert fastq_edge[1][1].file.path == "sample4.fastq"


@use_tempdir
def test_dataflow(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Automated dataflow extraction.
    """
    assert backend.session

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

    # Create dataset.
    fastqs = []
    for i in range(5):
        fastq = File("sample{}.fastq".format(i))
        fastq.write("reads{}".format(i))
        fastqs.append(fastq)

    ref_genome = File("ref_genome")
    ref_genome.write("genome")

    # Run workflow.
    scheduler.run(main(fastqs, ref_genome))

    # Ensure workflow works as intended.
    assert File("sample4.vcf").read() == "calls(align(reads4, genome), genome)"

    vcf = File("sample4.vcf")
    value = backend.session.query(Value).filter(Value.value_hash == vcf.get_hash()).one()

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))

    # Render dataflow as text.
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <********> call_variants(bam, ref_genome)
  bam        = <********> File(path=sample4.bam, hash=********)
  ref_genome = <********> File(path=ref_genome, hash=********)

bam <-- argument of <********> call_variants_all(bams, ref_genome)
    <-- <********> align_reads_all(fastqs, ref_genome)
    <-- <********> align_reads(fastq, ref_genome_2)
  fastq        = <********> File(path=sample4.fastq, hash=********)
  ref_genome_2 = <********> File(path=ref_genome, hash=********)

fastq <-- argument of <********> align_reads_all(fastqs, ref_genome)
      <-- argument of <********> main(fastqs, ref_genome)
      <-- origin

ref_genome_2 <-- argument of <********> align_reads_all(fastqs, ref_genome)
             <-- argument of <********> main(fastqs, ref_genome)
             <-- <********> ref_genome_3

ref_genome <-- argument of <********> call_variants_all(bams, ref_genome)
           <-- argument of <********> main(fastqs, ref_genome)
           <-- <********> ref_genome_3

ref_genome_3 <-- origin
""",
        output,
    )

    # Serialize dataflow.
    dom_json = list(serialize_dataflow(dom))

    # Assert dataflow serialization.
    expected_json = [
        {
            "args": [
                {
                    "hash": Match(str),
                    "value_display": Match(regex=r"File\(path=sample4.bam, hash=........\)"),
                    "var_name": "bam",
                },
                {
                    "hash": Match(str),
                    "value_display": Match(regex=r"File\(path=ref_genome, hash=........\)"),
                    "var_name": "ref_genome",
                },
            ],
            "assign": {
                "display": "call_variants(bam, ref_genome)",
                "hash": Match(str),
                "job_id": Match(str),
                "execution_id": Match(str),
                "prefix": "",
                "var_name": "value",
            },
            "routing": [],
        },
        {
            "args": [
                {
                    "hash": Match(str),
                    "value_display": Match(regex=r"File\(path=sample4.fastq, hash=........\)"),
                    "var_name": "fastq",
                },
                {
                    "hash": Match(str),
                    "value_display": Match(regex=r"File\(path=ref_genome, hash=........\)"),
                    "var_name": "ref_genome_2",
                },
            ],
            "assign": {
                "display": "call_variants_all(bams, ref_genome)",
                "hash": Match(str),
                "job_id": Match(str),
                "execution_id": Match(str),
                "prefix": "argument of",
                "var_name": "bam",
            },
            "routing": [
                {
                    "display": "align_reads_all(fastqs, ref_genome)",
                    "hash": Match(str),
                    "job_id": Match(str),
                    "execution_id": Match(str),
                    "prefix": "",
                },
                {
                    "display": "align_reads(fastq, ref_genome_2)",
                    "hash": Match(str),
                    "job_id": Match(str),
                    "execution_id": Match(str),
                    "prefix": "",
                },
            ],
        },
        {
            "args": [],
            "assign": {
                "display": "align_reads_all(fastqs, ref_genome)",
                "execution_id": Match(str),
                "hash": Match(str),
                "job_id": Match(str),
                "prefix": "argument of",
                "var_name": "fastq",
            },
            "routing": [
                {
                    "display": "main(fastqs, ref_genome)",
                    "execution_id": Match(str),
                    "hash": Match(str),
                    "job_id": Match(str),
                    "prefix": "argument of",
                },
                {
                    "display": "origin",
                    "execution_id": None,
                    "hash": None,
                    "job_id": None,
                    "prefix": "",
                },
            ],
        },
        {
            "args": [],
            "assign": {
                "display": "align_reads_all(fastqs, ref_genome)",
                "execution_id": Match(str),
                "hash": Match(str),
                "job_id": Match(str),
                "prefix": "argument of",
                "var_name": "ref_genome_2",
            },
            "routing": [
                {
                    "display": "main(fastqs, ref_genome)",
                    "execution_id": Match(str),
                    "hash": Match(str),
                    "job_id": Match(str),
                    "prefix": "argument of",
                },
                {
                    "display": "ref_genome_3",
                    "execution_id": Match(str),
                    "hash": Match(str),
                    "job_id": Match(str),
                    "prefix": "",
                },
            ],
        },
        {
            "args": [],
            "assign": {
                "display": "call_variants_all(bams, ref_genome)",
                "execution_id": Match(str),
                "hash": Match(str),
                "job_id": Match(str),
                "prefix": "argument of",
                "var_name": "ref_genome",
            },
            "routing": [
                {
                    "display": "main(fastqs, ref_genome)",
                    "execution_id": Match(str),
                    "hash": Match(str),
                    "job_id": Match(str),
                    "prefix": "argument of",
                },
                {
                    "display": "ref_genome_3",
                    "execution_id": Match(str),
                    "hash": Match(str),
                    "job_id": Match(str),
                    "prefix": "",
                },
            ],
        },
        {
            "args": [],
            "assign": {
                "display": "origin",
                "execution_id": None,
                "hash": None,
                "job_id": None,
                "prefix": "",
                "var_name": "ref_genome_3",
            },
            "routing": [],
        },
    ]

    for i, (record, expected_record) in enumerate(zip(dom_json, expected_json)):
        assert record == expected_record


def test_dataflow_routing(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Dataflow display should indicate argument and result routing.
    """
    assert backend.session

    @task()
    def task2(x: int) -> int:
        return x + 1

    @task()
    def task1(x: int) -> int:
        return task2(x)

    @task()
    def task1b(x: int) -> int:
        return x + 1

    @task()
    def main(x: int) -> int:
        return task1b(task1(x))

    # Run workflow.
    assert scheduler.run(main(10)) == 12

    exec1 = backend.session.query(Execution).one()
    value = exec1.call_node.value

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <********> task1b(x)
  x = <********> 11

x <-- <********> task1(x)
  <-- <********> task2(x_2)
  x_2 = <********> 10

x_2 <-- argument of <********> task1(x)
    <-- argument of <********> main(x)
    <-- origin
""",
        output,
    )


@use_tempdir
def test_dataflow_subvalue(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Dataflow should handle tracing subvalues.
    """
    assert backend.session

    @task()
    def task1(x: int) -> File:
        file = File(str(x))
        file.write(str(x))
        return file

    @task()
    def task2(input: File) -> File:
        file = File(input.path + ".v2")
        file.write(input.read())
        return file

    @task()
    def process_files(files: List[File]) -> List[File]:
        return [task2(file) for file in files]

    @task()
    def main(x: int) -> List[File]:
        files = [task1(x), task1(x + 1)]
        return process_files(files)

    # Run workflow.
    assert len(scheduler.run(main(10))) == 2

    exec1 = backend.session.query(Execution).one()
    value = exec1.call_node.value

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <********> process_files(files)
  files = <********> [File(path=10, hash=********), File(path=11, hash=********)]

files <-- derives from
  task1_result   = <********> File(path=11, hash=********)
  task1_result_2 = <********> File(path=10, hash=********)

task1_result <-- <********> task1(x)
  x = <********> 11

task1_result_2 <-- <********> task1(x_2)
  x_2 = <********> 10

x_2 <-- argument of <********> main(x)
    <-- origin
""",
        output,
    )


@use_tempdir
def test_dataflow_subvalue_mix(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Dataflow should handle tracing subvalues that mix.
    """
    assert backend.session

    @task()
    def get_files() -> List[File]:
        file1 = File("a")
        file1.write("a")
        file2 = File("b")
        file2.write("b")
        return [file1, file2]

    @task()
    def process_files(files: List[File]) -> List[str]:
        return [cast(str, file.read()) for file in files]

    @task()
    def main() -> List[str]:
        files = get_files()
        return process_files([files[1], files[0]])

    # Run workflow.
    assert scheduler.run(main()) == ["b", "a"]

    exec1 = backend.session.query(Execution).one()
    value = exec1.call_node.value

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <********> process_files(files)
  files = <********> [File(path=b, hash=********), File(path=a, hash=********)]

files <-- derives from
  get_files_result = <********> [File(path=a, hash=********), File(path=b, hash=********)]

get_files_result <-- <********> get_files()
                 <-- origin
""",
        output,
    )


@use_tempdir
def test_dataflow_derives(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Dataflow should handle tracing subvalues that fanout.
    """
    assert backend.session

    @task()
    def list_files(dir):
        return ["prog.c", "lib.c"]

    @task()
    def compile(c_file):
        return c_file.replace(".c", ".o")

    @task()
    def link(o_files):
        return o_files[0][:-2]

    @task()
    def main(dir) -> str:
        c_files = list_files(dir)[:2]
        o_files = [compile(c_file) for c_file in c_files]
        prog = link(o_files)
        return prog

    # Run workflow.
    assert scheduler.run(main("my-prog/")) == "prog"

    exec1 = backend.session.query(Execution).one()
    value = exec1.call_node.value

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <48147ca5> link(o_files)
  o_files = <960234eb> ['prog.o', 'lib.o']

o_files <-- derives from
  compile_result   = <9cdc943d> 'lib.o'
  compile_result_2 = <5da457e9> 'prog.o'

compile_result <-- <a561a571> compile(c_file)
  c_file = <13c0193f> 'lib.c'

c_file <-- derives from
  list_files_result = <49a8fc1c> ['prog.c', 'lib.c']

compile_result_2 <-- <abf16029> compile(c_file_2)
  c_file_2 = <751a22a8> 'prog.c'

c_file_2 <-- derives from
  list_files_result = <49a8fc1c> ['prog.c', 'lib.c']

list_files_result <-- <b9dea9ed> list_files(dir)
  dir = <dc00c649> 'my-prog/'

dir <-- argument of <2151d173> main(dir)
    <-- origin
""",
        output,
    )


@use_tempdir
def test_dataflow_parallel(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Dataflow should handle tracing subvalues that fanout.
    """
    assert backend.session

    @task()
    def task1(x: int) -> int:
        return x + 1

    @task()
    def task2a(x: int) -> int:
        return x + 1

    @task()
    def task2b(x: int) -> int:
        return x + 1

    @task()
    def task3(a: int, b: int) -> int:
        return a + b

    @task()
    def main(x: int) -> int:
        y = task1(x)
        z1 = task2a(y)
        z2 = task2b(y)
        return task3(z1, z2)

    # Run workflow.
    assert scheduler.run(main(10)) == 24

    exec1 = backend.session.query(Execution).one()
    value = exec1.call_node.value

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <25136331> task3(a, b)
  a = <eb88d6a0> 12
  b = <eb88d6a0> 12

a <-- <d2cb0298> task2a(x)
  x = <bfe03b79> 11

b <-- <8410db36> task2b(x_2)
  x_2 = <bfe03b79> 11

x <-- <cf9b2b45> x_3

x_2 <-- <cf9b2b45> x_3

x_3 <-- <cf9b2b45> task1(x_4)
  x_4 = <279a8e0b> 10

x_4 <-- argument of <b08130cb> main(x)
    <-- origin
""",
        output,
    )


@use_tempdir
def test_dataflow_subvalue_parallel(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Dataflow should handle tracing subvalues that route in parallel.
    """
    assert backend.session

    File("file1").write("a")
    File("file2").write("b")

    @task()
    def list_files() -> Dict[str, File]:
        return {
            "file1": File("file1"),
            "file2": File("file2"),
        }

    @task()
    def process_files(file1: File, file2: File) -> str:
        return cast(str, file1.read()) + cast(str, file2.read())

    @task()
    def main() -> str:
        files = list_files()
        return process_files(files["file1"], files["file2"])

    # Run workflow.
    assert scheduler.run(main()) == "ab"

    exec1 = backend.session.query(Execution).one()
    value = exec1.call_node.value

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <********> process_files(file1, file2)
  file1 = <********> File(path=file1, hash=********)
  file2 = <********> File(path=file2, hash=********)

file1 <-- derives from
  list_files_result = <********> {'file1': File(path=file1, hash=********), 'file2': File(path=file2, hash=********)}

file2 <-- derives from
  list_files_result = <********> {'file1': File(path=file1, hash=********), 'file2': File(path=file2, hash=********)}

list_files_result <-- <********> list_files()
                  <-- origin
""",  # noqa: E501
        output,
    )


@use_tempdir
def test_default_argument_names(scheduler: Scheduler, backend: RedunBackendDb) -> None:
    """
    Dataflow should use the right argument names for parameters with default values.
    """

    @task()
    def get_planet() -> str:
        return "World"

    @task()
    def greeter(greet: str, thing: str) -> str:
        return "{}, {}!".format(greet, thing)

    @task()
    def main(greet: str = "Hello") -> str:
        return greeter(greet, get_planet())

    # Run workflow.
    assert scheduler.run(main()) == "Hello, World!"

    assert backend.session is not None
    exec1 = backend.session.query(Execution).one()
    value = exec1.call_node.value

    # Change the source code of a task.
    task1 = backend.session.query(Task).filter(Task.name == "greeter").one()
    task1.source = "incorrectly formatted function"
    backend.session.commit()

    # Extract dataflow.
    dataflow_edges = list(walk_dataflow(backend, value))
    dom = list(make_dataflow_dom(dataflow_edges))
    output = "\n".join(display_dataflow(dom))

    # Assert dataflow text.
    assert_match_text(
        """\
value <-- <fad2f196> greeter(arg0, arg1)
  arg0 = <d5d4dead> 'Hello'
  arg1 = <58dc3a20> 'World'

arg0 <-- argument of <2d6d02a4> main(greet)
     <-- origin

arg1 <-- <634ba277> get_planet()
     <-- origin
""",
        output,
    )
