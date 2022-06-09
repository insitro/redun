"""
Tests Glue helper functions in `redun.glue`.
Uses a pytest fixture to provide a spark context.
Spark imports are kept within the tests to avoid changing the behavior of NamedTuple.
"""
import pytest

from redun import glue


@pytest.fixture()
def spark_session(request):
    """
    Returns a Spark Contex
    """
    from pyspark.sql import SparkSession

    return SparkSession.builder.master("local[*]").appName("redun_test").getOrCreate()


def test_udf_without_spark():
    """
    Without a Spark context, UDF should operate like a normal Python function
    """

    from pyspark.sql.types import ArrayType, IntegerType

    @glue.udf
    def noarg_udf(input1: str):
        return input1 + " potatoes"

    @glue.udf(return_type=ArrayType(IntegerType()))
    def udf_tester_func(input1: str, input2: str):
        return [int(input1), int(input2)]

    assert udf_tester_func("3", "2") == [3, 2]
    assert noarg_udf("i like") == "i like potatoes"


def test_udf_with_spark(spark_session):

    from pyspark.sql.column import Column
    from pyspark.sql.types import ArrayType, IntegerType

    @glue.udf(return_type=ArrayType(IntegerType()))
    def udf_tester_func2(input1: str, input2: str):
        return [int(input1), int(input2)]

    @glue.udf
    def noarg_udf(input1: str):
        return input1 + "hello"

    assert isinstance(udf_tester_func2("3", "2"), Column)

    df = spark_session.createDataFrame(
        [("a", "1", "2"), ("b", "2", "3"), ("ab", "0", "0")], schema=["name", "x", "y"]
    )
    df = df.withColumn("udf_col", udf_tester_func2(df.x, df.y))
    df = df.withColumn("str_col", noarg_udf(df.name))
    assert ("udf_col", "array<int>") in df.dtypes
    assert ("str_col", "string") in df.dtypes


def test_sql_query(spark_session):
    df = spark_session.createDataFrame(
        [("a" * i + "h", i) for i in range(10)], schema=["name", "count"]
    )

    # Test invalid alias
    with pytest.raises(ValueError, match=r"alias .* not referenced"):
        _ = glue.sql_query(df, "select max(length(name)) as longest from aliased_name")

    result = glue.sql_query(
        df, "select max(length(name)) as longest from aliased_name", dataset_alias="aliased_name"
    )

    assert result.collect()[0].asDict()["longest"] == 10
