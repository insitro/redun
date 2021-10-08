from redun import task
from redun.file import ShardedS3Dataset
from redun.glue import get_spark_context


redun_namespace = "redun.examples.aws_glue"


@task(executor="glue")
def process_data():
    sc = get_spark_context()

    data = [
        (x, x // 10, x * 100)
        for x in range(1000)
    ]
    rdd = sc.parallelize(data)
    df = rdd.toDF(["id", "shard", "value"])
    print(type(df))
    print(df)

    dataset = ShardedS3Dataset("s3://YOUR_BUCKET/tmp/dataset")
    dataset.save_spark(df, partition_keys=["shard"], catalog_table="rasmus_test")
    return dataset


@task()
def main():
    return process_data()
