# Utility routines for k8s API interactions
# This library contains standardized routines for interacting with k8s through its API
# It uses the Official Python client library for kubernetes:
# https://github.com/kubernetes-client/python
from kubernetes import client, config

from redun.executors.aws_utils import get_aws_env_vars

DEFAULT_JOB_PREFIX = "redun-job"


def get_k8s_batch_client():
    """returns an API client supporting k8s batch API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md"""
    config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    return batch_v1


def get_k8s_version_client():
    """returns an API client support k8s version API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/VersionApi.md"""
    config.load_kube_config()
    version = client.VersionApi()
    return version


def get_k8s_core_client():
    """returns an API client support k8s core API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md"""
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    return core_v1


def create_resources(requests=None, limits=None):
    """Creates resource limits for k8s pods
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ResourceRequirements.md
    """
    resources = client.V1ResourceRequirements()

    if requests is None:
        resources.requests = {}
    else:
        resources.requests = requests

    if limits is None:
        resources.limits = {}
    else:
        resources.limits = limits

    return resources


def create_job_object(
    name=DEFAULT_JOB_PREFIX,
    image="bash",
    command="false",
    resources=None,
    timeout=None,
    labels=None,
    uid=None,
    retries=1,
):
    """Creates a job object for redun job.
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    Also creates necessary sub-objects"""

    env = [{"name": key, "value": value} for key, value in get_aws_env_vars().items()]
    container = client.V1Container(name=name, image=image, command=command, env=env)

    if resources is None:
        container.resources = create_resources()
    else:
        container.resources = resources
    pod_spec = client.V1PodSpec(
        restart_policy="Never", image_pull_secrets=[{"name": "regcred"}], containers=[container]
    )

    # Create and configurate a pod template spec section
    template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(), spec=pod_spec)

    # Create the spec of job deployment
    spec = client.V1JobSpec(
        template=template,
        backoff_limit=retries,
        # ttl_seconds_after_finished=100,
    )
    if timeout:
        spec.active_deadline_seconds = timeout

    if labels is None:
        labels = {}

    # Instantiate the job object
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=name, labels=labels, uid=uid),
        spec=spec,
    )
    return job


def create_namespace(api_instance, namespace):
    """Create a k8s namespace job"""
    return api_instance.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))

def get_version(api_instance):
    api_instance = get_k8s_version_client()
    version_info = api_instance.get_code()
    major = int(version_info.major)
    minor = int(version_info.minor)
    return major, minor
    
def create_job(api_instance, job, namespace):
    """Create an actual k8s job"""
    api_response = api_instance.create_namespaced_job(body=job, namespace=namespace)
    return api_response


def delete_job(api_instance, name, namespace):
    """Delete an existing k8s job"""
    body = client.V1DeleteOptions()
    api_response = api_instance.delete_namespaced_job(name=name, namespace=namespace, body=body)
    return api_response
