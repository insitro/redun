# Utility routines for k8s API interactions
# This library contains standardized routines for interacting with k8s through its API
# It uses the Official Python client library for kubernetes:
# https://github.com/kubernetes-client/python
from base64 import b64encode
from typing import Any

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.config import ConfigException

from redun.executors.aws_utils import get_aws_env_vars
from redun.logging import logger

DEFAULT_JOB_PREFIX = "redun-job"


def load_k8s_config() -> None:
    """
    Load kubernetes config.
    """
    try:
        config.load_kube_config()
    except ConfigException:
        logger.warn(
            "config.load_kube_config() failed. Resorting to config.load_incluster_config()."
        )
        config.load_incluster_config()


def get_k8s_batch_client():
    """returns an API client supporting k8s batch API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md"""
    load_k8s_config()
    batch_v1 = client.BatchV1Api()
    return batch_v1


def get_k8s_version_client():
    """returns an API client support k8s version API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/VersionApi.md"""
    load_k8s_config()
    version = client.VersionApi()
    return version


def get_k8s_core_client():
    """returns an API client support k8s core API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md"""
    load_k8s_config()
    core_v1 = client.CoreV1Api()
    return core_v1


def delete_k8s_secret(secret_name: str, namespace: str) -> None:
    core_api = get_k8s_core_client()
    try:
        core_api.delete_namespaced_secret(secret_name, namespace)
    except ApiException as error:
        if error.status != 404:
            raise


def create_k8s_secret(secret_name: str, namespace: str, secret_data: dict) -> Any:
    # Delete existing secret if it exists.
    delete_k8s_secret(secret_name, namespace)

    # Build secret.
    metadata = {"name": secret_name, "namespace": namespace}
    data = {
        key: b64encode(value.encode("utf8")).decode("utf8") for key, value in secret_data.items()
    }
    body = client.V1Secret("v1", data, False, "Secret", metadata, type="Opaque")

    # Create secret.
    core_api = get_k8s_core_client()
    return core_api.create_namespaced_secret(namespace, body)


def import_aws_secrets(namespace: str = "default") -> None:
    create_k8s_secret("redun-aws", namespace, get_aws_env_vars())


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
    service_account_name="default",
    annotations=None,
):
    """Creates a job object for redun job.
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    Also creates necessary sub-objects"""

    # Container environment variables.
    env = []

    # Forward AWS secrets to the container environment variables.
    aws_env_keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
    env.extend(
        [
            {
                "name": key,
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "redun-aws",
                        "key": key,
                        "optional": True,
                    },
                },
            }
            for key in aws_env_keys
        ]
    )

    container = client.V1Container(name=name, image=image, command=command, env=env)

    if resources is None:
        container.resources = create_resources()
    else:
        container.resources = resources
    pod_spec = client.V1PodSpec(
        service_account_name=service_account_name,
        restart_policy="Never",
        image_pull_secrets=[{"name": "regcred"}],
        containers=[container],
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
        metadata=client.V1ObjectMeta(annotations=annotations, name=name, labels=labels, uid=uid),
        spec=spec,
    )
    return job


def create_namespace(api_instance, namespace):
    """Create a k8s namespace job"""
    return api_instance.create_namespace(
        client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    )


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
