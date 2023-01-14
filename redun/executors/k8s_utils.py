# Utility routines for k8s API interactions
# This library contains standardized routines for interacting with k8s through its API
# It uses the Official Python client library for kubernetes:
# https://github.com/kubernetes-client/python
from base64 import b64encode
from typing import Any, Dict, List, Optional, Tuple

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.config import ConfigException

from redun.logging import logger

DEFAULT_JOB_PREFIX = "redun-job"


class K8SClient:
    """
    This class manages multiple k8s clients and their config loading.
    """

    def __init__(self):
        self._is_loaded = False
        self._core: Optional[client.CoreV1Api] = None
        self._batch: Optional[client.BatchV1Api] = None

    def load_config(self) -> None:
        """
        Loads k8s config if not loaded.
        """
        if not self._is_loaded:
            try:
                config.load_kube_config()
            except ConfigException:
                logger.warn(
                    "config.load_kube_config() failed. "
                    "Resorting to config.load_incluster_config()."
                )
                config.load_incluster_config()
            self._is_loaded = True

    def version(self) -> Tuple[int, int]:
        """
        Returns an API client support k8s version API.

        https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/VersionApi.md
        """
        self.load_config()
        version_info = client.VersionApi().get_code()
        major = int(version_info.major.replace("+", ""))
        minor = int(version_info.minor.replace("+", ""))
        return major, minor

    @property
    def core(self) -> client.CoreV1Api:
        """
        Returns an API client support k8s core API.

        https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md
        """
        if not self._core:
            self.load_config()
            self._core = client.CoreV1Api()
        return self._core

    @property
    def batch(self) -> client.BatchV1Api:
        """
        Returns an API client supporting k8s batch API.

        https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md
        """
        if not self._batch:
            self.load_config()
            self._batch = client.BatchV1Api()
        return self._batch


def delete_k8s_secret(k8s_client: K8SClient, secret_name: str, namespace: str) -> None:
    """
    Deletes a k8s Secret. If Secret doesn't exist, it does not throw an error.
    """
    try:
        k8s_client.core.delete_namespaced_secret(secret_name, namespace)
    except ApiException as error:
        if error.status != 404:
            # If secret doesn't exist, then this is a no-op.
            raise


def create_k8s_secret(
    k8s_client: K8SClient,
    secret_name: str,
    namespace: str,
    secret_data: dict,
    secret_type: str = "Opaque",
) -> Any:
    """
    Creates a k8s Secret from a dict of key-value pairs.
    """
    # Build secret.
    metadata = {"name": secret_name, "namespace": namespace}
    data = {
        key: b64encode(value.encode("utf8")).decode("utf8") for key, value in secret_data.items()
    }
    body = client.V1Secret("v1", data, False, "Secret", metadata, type=secret_type)

    # Create secret.
    try:
        return k8s_client.core.create_namespaced_secret(namespace, body)
    except ApiException as error:
        if error.status == 409:
            # Secret already exists, just patch it.
            return k8s_client.core.replace_namespaced_secret(secret_name, namespace, body)
        raise


def create_resources(
    requests: Optional[dict] = None, limits: Optional[dict] = None
) -> client.V1ResourceRequirements:
    """
    Creates resource limits for k8s pods.

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
    name: str = DEFAULT_JOB_PREFIX,
    image: str = "bash",
    command: List[str] = ["false"],
    resources: Optional[client.V1ResourceRequirements] = None,
    timeout: Optional[int] = None,
    labels: Optional[dict] = None,
    uid: Optional[str] = None,
    retries: int = 1,
    service_account_name: Optional[str] = "default",
    annotations: Optional[Dict[str, str]] = None,
    secret_name: Optional[str] = None,
) -> client.V1Job:
    """Creates a job object for redun job.
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    Also creates necessary sub-objects"""

    # Container environment variables.
    env = []

    # Forward AWS secrets to the container environment variables.
    aws_env_keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
    if secret_name:
        env.extend(
            [
                {
                    "name": key,
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": secret_name,
                            "key": key,
                            "optional": True,
                        },
                    },
                }
                for key in aws_env_keys
            ]
        )

    container = client.V1Container(name=name, image=image, args=command, env=env)

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
    return client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(annotations=annotations, name=name, labels=labels, uid=uid),
        spec=spec,
    )


def create_namespace(k8s_client: K8SClient, namespace: str) -> None:
    """Create a k8s namespace job"""
    try:
        k8s_client.core.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
        )
    except client.exceptions.ApiException as error:
        if error.status == 409 and error.reason == "Conflict":
            pass
        else:
            logger.error("Unexpected exception creating namespace", error.body)
            raise


def create_job(k8s_client: K8SClient, job: client.V1Job, namespace: str) -> client.V1Job:
    """
    Creates an actual k8s job.
    """
    try:
        return k8s_client.batch.create_namespaced_job(body=job, namespace=namespace)
    except client.exceptions.ApiException as error:
        if error.status == 409 and error.reason == "Conflict":
            # Job already exsists, return it.
            return k8s_client.batch.read_namespaced_job(job.metadata.name, namespace=namespace)
        else:
            logger.error("Error submitting k8s job:", error.body)
            raise


def delete_job(k8s_client: K8SClient, name: str, namespace: str) -> Any:
    """
    Deletes an existing k8s job.
    """
    return k8s_client.batch.delete_namespaced_job(
        name=name,
        namespace=namespace,
        body=client.V1DeleteOptions(propagation_policy="Background"),
    )
