from redun import task

redun_namespace = "redun.examples.startup.workflow_lib.utils"

ANALYSIS_IMAGE = "YOUR_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com/redun_aws_batch_example"
POSTPROCESS_IMAGE = "YOUR_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com/redun_aws_batch_example"


@task(executor="analysis_executor", image=ANALYSIS_IMAGE, memory=1)
def analysis(data: str):
    """
    Example of a task provided by a workflow library.

    Task-specific details such as the Docker image and memory needs are defined
    here on the task. However, it is left to the user to define the executor
    'utils_executor', so that user can customize project-specific details such
    as batch queue, and job roles.
    """
    return f"analysis({data})"


@task(executor="postprocess_executor", image=POSTPROCESS_IMAGE, memory=2)
def postprocess_data(data: str):
    """
    Example of a task provided by a workflow library.

    Task-specific details such as the Docker image and memory needs are defined
    here on the task. However, it is left to the user to define the executor
    'utils_executor', so that user can customize project-specific details such
    as batch queue, and job roles.
    """
    return f"postprocess_data({data})"
