from redun.config import Config
from redun.executors.aws_batch import AWSBatchExecutor


class Account1Executor(AWSBatchExecutor):
    """
    AWSBatchExecutor for a particular AWS account.

    Example of a predefined executor that can be used by projects using this library.
    """

    def __init__(self, name: str):
        config = Config({
            "executors.account1_executor": {
                "image": f"YOUR_ACCOUNT1_ID.dkr.ecr.us-west-2.amazonaws.com/redun_example",
                "queue": "YOUR_QUEUE_NAME1",
                "s3_scratch": "s3://EXAMPLE_BUCKET1/redun/",
                "job_name_prefix": "redun-example",
            }
        })
        super().__init__(name, config=config["executors"]["account1_executor"])


class Account2Executor(AWSBatchExecutor):
    """
    AWSBatchExecutor for another AWS account.

    Example of a predefined executor that can be used by projects using this library.
    """

    def __init__(self, name: str):
        config = Config({
            "executors.account2_executor": {
                "image": f"YOUR_ACCOUNT2_ID.dkr.ecr.us-west-2.amazonaws.com/redun_example",
                "queue": "YOUR_QUEUE_NAME2",
                "s3_scratch": "s3://EXAMPLE_BUCKET2/redun/",
                "job_name_prefix": "redun-example",
            }
        })
        super().__init__(name, config=config["executors"]["account2_executor"])
