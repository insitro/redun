"""
Helper functions for glue jobs.

Glue related imports are kept isolated, so this can be imported
people's local machines without issue, and then they call the
functions that will run when the context is defined.
"""
import functools
import typing
from typing import Callable, List, Optional

if typing.TYPE_CHECKING:
    from awsglue.context import GlueContext
    from awsglue.job import Job as GlueJob
    from pyspark.context import SparkContext
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import DataType


def setup_glue_job(job_name: str, job_args: List[str]) -> "GlueJob":

    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job as GlueJob
        from pyspark.context import SparkContext
    except (ImportError, ModuleNotFoundError):
        raise ValueError("Glue not installed. Are you running locally?")

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    job = GlueJob(glue_context)
    job.init(job_name, job_args)

    return job


def get_glue_context() -> "GlueContext":
    """
    Returns the current glue context
    """
    try:
        from awsglue.context import GlueContext
    except (ImportError, ModuleNotFoundError):
        raise ValueError("Glue not installed. Are you running locally?")

    sc = get_spark_context()
    return GlueContext(sc)


def get_spark_context() -> "SparkContext":
    """
    Returns the current spark context.
    """
    try:
        from pyspark.context import SparkContext
    except (ImportError, ModuleNotFoundError):
        raise ValueError("Spark not installed. Are you running locally?")

    # Raise an error if no context is defined, as Spark should already be
    # instantiated. We don't want to create a new one locally on accident.
    if SparkContext._active_spark_context is None:
        raise ValueError("Spark has not been initialized.")

    # Let Spark validate the available context for us.
    sc = SparkContext.getOrCreate()
    return sc


def get_spark_session() -> "SparkSession":
    try:
        from pyspark.sql import SparkSession
    except (ImportError, ModuleNotFoundError):
        raise ValueError("Spark not installed. Are you running locally?")

    # Session will be None if no context is available.
    session = SparkSession.getActiveSession()

    if session is None:
        raise ValueError("Spark has not been initialized.")

    return session


def get_num_workers() -> int:
    """
    Returns the number of workers in the current Spark context.
    """
    workers = get_spark_context().getConf().get("spark.executor.instances")
    return int(workers)


def udf(wrapped_func: Callable = None, return_type: Optional["DataType"] = None) -> Callable:
    """
    Creates a spark user defined function.
    Wraps `pyspark.sql.functions.udf` so spark context is only needed at
    runtime rather than redun task expression time.
    """
    if wrapped_func is None:
        return functools.partial(udf, return_type=return_type)

    try:
        get_spark_session()
    except ValueError:
        return wrapped_func

    from pyspark.sql.functions import udf as pyspark_udf

    if return_type is None:
        return pyspark_udf(wrapped_func)

    return pyspark_udf(wrapped_func, return_type)


def clean_datacatalog_table(database: str, table: str, remove_older_than: int = 1) -> None:
    """
    Removes records and files from a datacatalog table older than
    `remove_older_than` hours. Underlying data on S3 will be deleted, too.
    """
    context = get_glue_context()
    context.purge_table(
        catalog_id=None,
        database=database,
        table=table,
        options={"retentionPeriod": remove_older_than},
    )


def load_datacatalog_spark_dataset(
    database: str, table_name: str, hashfield: Optional[str] = None
) -> "DataFrame":
    """
    Loads a dataset from DataCatalog
    """
    context = get_glue_context()
    options = {"hashfield": hashfield} if hashfield else {}
    dataset = context.create_dynamic_frame.from_catalog(database, table_name, options)
    return dataset.toDF()


def sql_query(dataset: "DataFrame", query: str, dataset_alias: str = "dataset") -> "DataFrame":
    """
    Runs a SQL-style query on a Spark DataFrame.

    Parameters
    ----------
    dataset : pyspark.sql.DataFrame
        The dataset to query.

    query : str
        SQL query string.

    dataset_alias : str
        Name for dataset in SQL context. Defaults to "dataset".
        The SQL query should reference the alias like `"SELECT * FROM {dataset_alias}"`
    """

    if dataset_alias not in query:
        raise ValueError(f"Dataset alias '{dataset_alias}' not referenced in query: '{query}'")

    dataset.createOrReplaceTempView(dataset_alias)
    session = get_spark_session()
    return session.sql(query)
