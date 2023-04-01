from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from statistics import mean
from typing import Dict, Iterable, List, Optional, Tuple, Union

import requests

from google.cloud import batch_v1


# List of supported available CPU Platforms
# https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform#availablezones
class MinCPUPlatform(Enum):
    XEON_ICE_LAKE = "Intel Ice Lake"
    XEON_CASCADE_LAKE = "Intel Cascade Lake"
    XEON_SKYLAKE = "Intel Skylake"
    XEON_BROADWELL = "Intel Broadwell"
    XEON_HASWELL = "Intel Haswell"
    XEON_SANDY_BRIDGE = "Intel Sandy Bridge"
    EPYC_ROME = "AMD Rome"
    EPYC_MILAN = "AMD Milan"


def get_gcp_client(
    sync: bool = True,
) -> Union[batch_v1.BatchServiceClient, batch_v1.BatchServiceAsyncClient]:
    return batch_v1.BatchServiceClient() if sync else batch_v1.BatchServiceAsyncClient()

def gb_to_mib(gb):
    # Convert GiB to MiB.
    return int(gb * 1024)

def batch_submit(
    client: Union[batch_v1.BatchServiceClient, batch_v1.BatchServiceAsyncClient],
    job_name: str,
    project: str,
    region: str,
    machine_type: Optional[str],
    vcpus: int,
    memory: int,
    retries: int,
    priority: int,
    max_duration: str = None,
    task_count: int = 1,
    mount_buckets: List[str] = [],
    container_volumes: List[str] = [],
    container_options: List[str] = [],
    boot_disk_size_gib: int = None,
    min_cpu_platform: MinCPUPlatform = None,
    accelerators: List[Tuple[str, int]] = [],
    provisioning_model: str = "standard",
    image: str = None,
    script: str = "exit 0",
    commands: List[str] = ["exit 0"],
    service_account_email: str = "",
    labels: Dict[str, str] = {},
    **kwargs,  # Ignore extra args
) -> batch_v1.Job:
    # Define what will be done as part of the job.
    runnable = batch_v1.Runnable()

    # Setup volumes
    def make_volume(bucket):
        gcs_bucket = batch_v1.GCS()
        gcs_bucket.remote_path = bucket
        gcs_volume = batch_v1.Volume()
        gcs_volume.gcs = gcs_bucket
        gcs_volume.mount_path = f"/mnt/disks/{bucket}"
        return gcs_volume

    volumes = [make_volume(bucket) for bucket in mount_buckets]

    if image is None:
        runnable.script = batch_v1.Runnable.Script()
        runnable.script.text = script
        # You can also run a script from a file. Just remember, that needs to be a script that's
        # already on the VM that will be running the job. Using runnable.script.text and
        # runnable.script.path is mutually exclusive.
        # runnable.script.path = '/tmp/test.sh'
    else:
        runnable.container = batch_v1.Runnable.Container()
        runnable.container.image_uri = image
        runnable.container.commands = commands
        # TODO: is this needed?
        # runnable.container.options = "".join(
        #     [f" -v {x.mount_path}:{x.mount_path}" for x in volumes]
        # )
        runnable.container.options = " ".join(container_options)
        runnable.container.volumes = container_volumes
        runnable.container.volumes += [f"{x.mount_path}:{x.mount_path}" for x in volumes]
        # TODO: this causes an error, going back to the above
        # runnable.container.volumes = [','.join([f"{x.mount_path}:{x.mount_path}" for x in volumes])]

    task = batch_v1.TaskSpec()
    task.runnables = [runnable]
    task.volumes = volumes

    # We can specify what resources are requested by each task.
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = vcpus * 1000  # in milliseconds per cpu-second.
    # This means the task requires 2 whole CPUs with default value.
    resources.memory_mib = gb_to_mib(memory)
    if boot_disk_size_gib:
        resources.boot_disk_mib = gb_to_mib(boot_disk_size_gib)
    task.compute_resource = resources

    task.max_retry_count = retries

    if max_duration:
        task.max_run_duration = max_duration

    # Tasks are grouped inside a job using TaskGroups.
    # Currently, it's possible to have only one task group.
    group = batch_v1.TaskGroup()
    group.task_count = task_count
    group.task_spec = task

    # Policies are used to define on what kind of virtual machines the tasks will run on.
    # Read more about machine types here: https://cloud.google.com/compute/docs/machine-types
    if not machine_type:
        gpus = sum([a[1] for a in accelerators])
        machine_type = find_best_matching_machine_type(
            cpus=vcpus, memory=memory, region=region,
            spot=provisioning_model == "spot", local_ssd=False,
            gpus=gpus, min_cpu_platform=min_cpu_platform
        )
    allocation_policy = batch_v1.AllocationPolicy()
    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = machine_type
    policy.min_cpu_platform = min_cpu_platform
    policy.provisioning_model = (
        batch_v1.AllocationPolicy.ProvisioningModel.SPOT
        if provisioning_model == "spot"
        else batch_v1.AllocationPolicy.ProvisioningModel.STANDARD
    )

    def create_accelerator(type, count):
        accelerator = batch_v1.AllocationPolicy.Accelerator()
        accelerator.type_ = type
        accelerator.count = count
        return accelerator

    policy.accelerators = list(map(lambda a: create_accelerator(a[0], a[1]), accelerators))

    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    if policy.accelerators:
        instances.install_gpu_drivers = True
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

    # We use Cloud Logging as it's an out of the box available option
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    create_request = batch_v1.CreateJobRequest()
    create_request.job = job
    create_request.job_id = job_name
    # The job's parent is the region in which the job will run
    create_request.parent = f"projects/{project}/locations/{region}"

    return client.create_job(create_request)


def list_jobs(
    client: batch_v1.BatchServiceClient, project_id: str, region: str
) -> Iterable[batch_v1.Job]:
    """
    Get a list of all jobs defined in given region.

    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        region: name of the region hosting the jobs.

    Returns:
        An iterable collection of Job object.
    """
    return client.list_jobs(parent=f"projects/{project_id}/locations/{region}")


def format_job_name(project_id: str, region: str, job_name: str) -> str:
    """
    Create a job name from provided details

    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        region: name of the region hosts the job.
        job_name: fully qualified name of the job you want to retrieve information about.

    Returns:
        A formatted job name.
    """
    return f"projects/{project_id}/locations/{region}/jobs/{job_name}"


def get_job(client: batch_v1.BatchServiceClient, job_name: str) -> batch_v1.Job:
    """
    Retrieve information about a Batch Job.

    Args:
        job_name: fully qualified name of the job you want to retrieve information about.

    Returns:
        A Job object representing the specified job.
    """
    return client.get_job(name=job_name)


def format_task_group_name(project_id: str, region: str, job_name: str, group_name: str) -> str:
    """
    Create a task group name from provided details

    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        region: name of the region hosting the jobs.
        job_name: name of the job which tasks you want to list.
        group_name: name of the group of tasks. Usually it's `group0`.

    Return:
        A formatted task group name.
    """
    return f"projects/{project_id}/locations/{region}/jobs/{job_name}/taskGroups/{group_name}"


def list_tasks(client: batch_v1.BatchServiceClient, group_name: str) -> Iterable[batch_v1.Task]:
    """
    Get a list of all jobs defined in given region.

    Args:
        group_name: fully qualified name of the group of tasks you want to look up

    Returns:
        An iterable collection of Task objects.
    """
    return client.list_tasks(parent=group_name)


def format_task_name(
    project_id: str, region: str, job_name: str, group_name: str, task_number: int
) -> str:
    """
    Create a task name from provided details

    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        region: name of the region hosts the job.
        job_name: the name of the job you want to retrieve information about.
        group_name: the name of the group that owns the task you want to check.
            Usually it's `group0`.
        task_number: number of the task you want to look up.

    Return:
        A formatted task name.
    """
    return (
        f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        f"/taskGroups/{group_name}/tasks/{task_number}"
    )


def get_task(client: batch_v1.BatchServiceClient, task_name: str) -> batch_v1.Task:
    """
    Retrieve information about a Task.

    Args:
        task_name: fully qualified name of the task you want to look up.

    Returns:
        A Task object representing the specified task.
    """
    return client.get_task(name=task_name)


# largely based on nextflow's implementation: https://github.com/nextflow-io/nextflow/blob/master/plugins/nf-google/src/main/nextflow/cloud/google/batch/GoogleBatchMachineTypeSelector.groovy
@dataclass
class MachineType:
    type: str
    family: str
    spot_price: float
    on_demand_price: float
    cpus: int
    memory: int
    gpus: int


@lru_cache()
def get_available_machine_types(region: str) -> List[MachineType]:
    CLOUD_INFO_API = "https://cloudinfo.seqera.io/api/v1"
    API_URL = f"{CLOUD_INFO_API}/providers/google/services/compute/regions/{region}/products"
    request = requests.get(API_URL, timeout=30)
    if request.status_code != 200:
        raise RuntimeError(f"Error getting machine types from url '{API_URL}'!\nSpecify machine type manually.")
    result: List[Dict] = request.json()["products"]
    return [
        MachineType(
            type=p["type"],
            family=p["type"].split("-")[0],
            spot_price=mean(e["price"] for e in p["spotPrice"]),
            on_demand_price=p["onDemandPrice"],
            cpus=p["cpusPerVm"],
            memory=p["memPerVm"],
            gpus=p["gpusPerVm"],
        )
        for p in result
    ]


def find_best_matching_machine_type(
    cpus: int,
    memory: int,
    region: str,
    spot: bool,
    local_ssd: bool,
    gpus: int = 0,
    families: Optional[List[str]] = None,
    min_cpu_platform: Optional[MinCPUPlatform] = None,
) -> str:
    if gpus > 0:
        # seqera's cloudinfo doesn't have GPU prices yet
        raise NotImplementedError("Automatic machine type selection is not implemented yet for GPU instances. Specify machine type manually.")
    FAMILY_COST_CORRECTION = {
        "e2": 1.0,  # Mix of processors, tend to be similar in performance to N1
        # INTEL
        "n1": 1.0,  # Skylake, Broadwell, Haswell, Sandy Bridge, and Ivy Bridge ~2.7 Ghz
        "n2": 0.85,  # Intel Xeon Gold 6268CL ~3.4 Ghz
        "c2": 0.8,  # Intel Xeon Gold 6253CL ~3.8 Ghz
        "m1": 1.0,  # Intel Xeon E7-8880V4 ~2.7 Ghz
        "m2": 0.85,  # Intel Xeon Platinum 8280L ~3.4 Ghz
        "m3": 0.85,  # Intel Xeon Platinum 8373C ~3.4 Ghz
        "a2": 0.9,  # Intel Xeon Platinum 8273CL ~2.9 Ghz
        # AMD
        "t2d": 1.0,  # AMD EPYC Milan ~2.7 Ghz
        "n2d": 1.0,  # AMD EPYC Milan ~2.7 Ghz
    }
    FAMILIES_WITH_LOCAL_SSD = ["n1", "n2", "n2d", "c2", "c2d", "m3"]
    NO_MIN_CPU_PLATFORM = ["e2-micro", "e2-small", "e2-medium", "f1-micro", "g1-small"]

    machine_types = get_available_machine_types(region)
    if not families:
        families = []
    if len(families) == 1:
        family_or_type = families[0]
        if "custom-" in family_or_type:
            return family_or_type
        if any(e.type == family_or_type for e in machine_types):
            return family_or_type
    # restrict to families with local ssd
    if not families and local_ssd:
        families = FAMILIES_WITH_LOCAL_SSD

    if min_cpu_platform:
        # restrict to machine types with at least the specified min cpu platform
        machine_types = [
            e
            for e in machine_types
            if all(not e.type.startswith(no_min_cpu) for no_min_cpu in NO_MIN_CPU_PLATFORM)
        ]

    valid_machine_types = [
        e
        for e in machine_types
        if e.cpus >= cpus and e.memory >= memory and e.gpus >= gpus
        # either no family restriction or family has to at least start with one of the families
        and (not families or any(e.family.startswith(fam) for fam in families))
    ]

    min_cost_machine = min(
        valid_machine_types,
        key=lambda e: (
            FAMILY_COST_CORRECTION.get(e.family, 1) if e.cpus > 2 or e.memory > 2 else 1
        )
        * (e.spot_price if spot else e.on_demand_price),
    )

    return min_cost_machine.type
