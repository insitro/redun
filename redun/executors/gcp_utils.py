from typing import Iterable

from google.cloud import batch_v1


def get_gcp_client() -> batch_v1.BatchServiceClient:
    return batch_v1.BatchServiceClient()


def batch_submit(
    job_name: str,
    project: str,
    region: str,
    gcs_bucket: str,
    mount_path: str,
    machine_type: str,
    vcpus: int,
    memory: int,
    task_count: int,
    max_duration: str,
    retries: int,
    priority: int,
    image: str = None,
    command: str = "exit 0",
    service_account_email: str = "",
    labels: dict[str, str] = {},
) -> batch_v1.Job:

    client = batch_v1.BatchServiceClient()

    # Define what will be done as part of the job.
    runnable = batch_v1.Runnable()
    if image is None:
        runnable.script = batch_v1.Runnable.Script()
        runnable.script.text = command
        # You can also run a script from a file. Just remember, that needs to be a script that's
        # already on the VM that will be running the job. Using runnable.script.text and
        # runnable.script.path is mutually exclusive.
        # runnable.script.path = '/tmp/test.sh'
    else:
        runnable.container = batch_v1.Runnable.Container()
        runnable.container.image_uri = image
        runnable.container.entrypoint = "/bin/sh"
        runnable.container.commands = [command]

    task = batch_v1.TaskSpec()
    task.runnables = [runnable]

    # We can specify what resources are requested by each task.
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = vcpus * 1000  # in milliseconds per cpu-second.
    # This means the task requires 2 whole CPUs with default value.
    resources.memory_mib = memory
    task.compute_resource = resources

    task.max_retry_count = retries
    task.max_run_duration = max_duration

    # Tasks are grouped inside a job using TaskGroups.
    # Currently, it's possible to have only one task group.
    group = batch_v1.TaskGroup()
    group.task_count = task_count
    group.task_spec = task

    # Policies are used to define on what kind of virtual machines the tasks will run on.
    # In this case, we tell the system to use "e2-standard-4" machine type.
    # Read more about machine types here: https://cloud.google.com/compute/docs/machine-types
    allocation_policy = batch_v1.AllocationPolicy()
    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = machine_type
    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy.instances = [instances]

    if service_account_email:
        service_account = batch_v1.ServiceAccount()
        service_account.email = service_account_email
        allocation_policy.service_account = service_account

    job = batch_v1.Job()
    job.priority = priority
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.labels = labels
    if image is None:
        job.labels["redun_job_type"] = "script"
    else:
        job.labels["redun_job_type"] = "container"

    # We use Cloud Logging as it's an out of the box available option
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    create_request = batch_v1.CreateJobRequest()
    create_request.job = job
    create_request.job_id = job_name
    # The job's parent is the region in which the job will run
    create_request.parent = f"projects/{project}/locations/{region}"

    return client.create_job(create_request)


def list_jobs(project_id: str, region: str) -> Iterable[batch_v1.Job]:
    """
    Get a list of all jobs defined in given region.

    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        region: name of the region hosting the jobs.

    Returns:
        An iterable collection of Job object.
    """
    client = batch_v1.BatchServiceClient()

    return client.list_jobs(parent=f"projects/{project_id}/locations/{region}")
