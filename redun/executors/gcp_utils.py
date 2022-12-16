from google.cloud import batch_v1


def get_gcp_client() -> batch_v1.BatchServiceClient:
    return batch_v1.BatchServiceClient()


def batch_submit(
    project: str,
    region: str,
    job_name: str = "gcp-batch-job",
    image: str = None,  # "gcr.io/google-containers/busybox"
    command: str = "exit 0",
    machine_type: str = "e2-standard-4",
    memory: int = 16,
    vcpus: int = 2,
    task_count: int = 1,
    retries: int = 2,
    max_duration: str = "3600s",
    labels: dict[str, str] = {},
) -> batch_v1.Job:

    client = batch_v1.BatchServiceClient()

    # Define what will be done as part of the job.
    task = batch_v1.TaskSpec()
    runnable = batch_v1.Runnable()
    if image is None:
        runnable.script = batch_v1.Runnable.Script()
        runnable.script.text = command
        # You can also run a script from a file. Just remember, that needs to be a script that's
        # already on the VM that will be running the job. Using runnable.script.text and
        # runnable.script.path is mutually exclusive.
        # runnable.script.path = '/tmp/test.sh'
        task.runnables = [runnable]
    else:
        runnable.container = batch_v1.Runnable.Container()
        runnable.container.image_uri = image
        runnable.container.entrypoint = "/bin/sh"
        runnable.container.commands = ["-c", command]

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

    job = batch_v1.Job()
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
