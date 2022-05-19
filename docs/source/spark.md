---
tocpdeth: 3
---

# Spark

Redun can now launch jobs using [AWS Glue](https://aws.amazon.com/glue) to run
[Apache Spark](https://spark.apache.org) jobs on a managed cluster. Spark jobs run on many
CPUs and are especially useful for working with large tabular datasets such as those
represented in pandas.

## Usage

### Data I/O

Spark datasets are typically saved as multiple files (shards) on S3. The [ShardedS3Dataset](redun/redun.html?#redun.file.ShardedS3Dataset) class represents these datasets in redun while tracking their provenance. Datasets may be
in a variety of formats, including parquet, csv, and avro. The default is always parquet.

Useful class functions, briefly:

- `load_spark`: Returns the dataset as a `pyspark.sql.DataFrame`
- `save_spark`: Saves pandas or pyspark `DataFrame`. 
- `load_pandas`: Loads one or more shards as a `pandas.DataFrame`

The hash of the dataset is the hash of the filenames that would be loaded by the next call to
`ShardedS3Dataset.load_spark`.

For example, this tasks uses `ShardedS3Dataset` to convert a dataset from csv to parquet.
```py
from redun import task, ShardedS3Dataset

@task(executor="glue")
def convert_dataset(input_csv_dataset: ShardedS3Dataset, output_dir: str) -> ShardedS3Dataset:
    output = ShardedS3Dataset(output_dir, format="parquet")

    loaded = input_csv_dataset.load_spark()
    output.save_spark(loaded)
    return output
```
Note that the number of shards and file names are determined by AWS Glue and not user configurable.

### Imported modules

AWS Glue automates management, provisioning, and deployment of Spark clusters, but only with a [pre-determined set of Python modules](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#glue20-modules-provided).
Most functionality you may need is already available, including scipy, pandas, etc.

Additional modules that are available in the public PyPi repository can be
installed with the `additional_libs` task option.  Redun is always available in
the glue environment.  However, other modules, especially those using C/C++
compiled extensions, are not really installable at this time. 

#### Rdkit
An exception to this is the `rdkit` module, which has been specially prepared to work in AWS Glue.
To use `rdkit`, you must call `redun.contrib.spark_helpers.enable_rdkit` from your glue task before
importing it.

This example uses `rdkit` and installs the `potato` module via pip.
```py
from redun.contrib.spark_helpers import enable_rdkit

@task(executor="glue", additional_libs=["potato"])
def use_rdkit():
    enable_rdkit()
    from rdkit import Chem
    import potato
    # ... compute goes here ...
```

#### Avoiding import errors
In a file with multiple redun tasks, packages are often imported at the top-level that are needed by non-glue tasks.
However, when the glue task is launched, those top-level imports may cause the job to fail with ImportError.

For example, the glue task here will fail due to being unable to import the package `foo`:

```py
from redun import task
import foo

@task(executor="batch")
def use_foo():
    return foo.bar()

@task(executor="glue")
def do_stuff():
    pass
```

To avoid this issue, you can either put the glue task in its own file with only the necessary imports, or
perform required imports for each task within the function body:

```py
from redun import task

@task(executor="batch")
def use_foo():
    import foo
    return foo.bar()

@task(executor="glue")
def do_stuff():
    pass
```


### Accessing Spark contexts

The `redun.glue` module contains helper functions to access the Spark Session and Context objects:
`get_spark_context()` and `get_spark_session()`. [Full Documentation](redun/redun.md#module-redun.glue)

### User-defined functions

You might want to define your own functions to operate on a dataset. Typically, you'd use the `pyspark.sql.functions.udf` decorator
on a function to make it a UDF, but when redun evaluates the decorator it will error out as there is no spark context available
to register the function to.

Instead, use the `redun.glue.udf` decorator. See the redun examples for a
real-world use of UDFs and this decorator.

### Monitoring Spark jobs

Glue/Spark jobs will log detailed progress information for visualization by a Spark History Server.
Logs will be at the location specified by the config option `spark_history_dir`.



## Common pitfalls

### Some workers keep dying and my task fails?

Repetitive failures can be caused by one worker getting overloaded with data and
running out of memory. Signs of this include large numbers of executors failing as
seen in Spark UI.

By default, the number of partitions is 200. The ideal number is number
of workers * number of cores, provided it all fits in memory. The default
seems to work most of the time for me.

You can manually repartition the dataset when it's a spark DataFrame.
To do this:

```py
spark_df = spark_df.repartition(400)
```
