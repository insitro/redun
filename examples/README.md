# redun guided examples

If you are learning redun for the first time, you can explore the following example projects in the order listed below. They have been arranged to introduce redun's features and concepts in increasing order of complexity. For a deeper explanation of the features and theory see the [redun design docs](https://insitro.github.io/redun/design.html).

0. [INSTALL](../README.md#install): Install the redun library and command-line program using `pip` or `conda`.

1. [01_hello_world](01_hello_world/): First toy workflow example. Introduces concepts of caching, argument parsing, incremental compute, and exploring the data provenance call graph.

2. [02_compile](02_compile/): Example workflow involving files (compiling c programs) and parallelism (fan-out, fan-in).

3. [03_scheduler](03_scheduler/): A Jupyter notebook-driven example of redun's approach to defining workflows with expression graphs, as opposed to dataflow DAGs (directed acyclic graphs). Expression graphs can enable more expressive and composable workflows in a very natural syntax.

4. [04_script](04_script/): Use the `script()` task to easily call shell scripts as well as stage and unstage files for input/output.

5. [05_aws_batch](05_aws_batch/): Define executors to run tasks on AWS Batch or within local Docker containers.

6. [06_bioinfo_batch](06_bioinfo_batch/): Full bioinformatics workflow for aligning sequencings on AWS Batch.
