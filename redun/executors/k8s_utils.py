# Utility routines for k8s API interactions
# This library contains standardized routines for interacting with k8s through its API
# It uses the Official Python client library for kubernetes:
# https://github.com/kubernetes-client/python
from kubernetes import client, config

DEFAULT_JOB_PREFIX = "redun-job"


# # CompletedIndexes holds the completed indexes when .spec.completionMode = "Indexed" in a text format.
# # The indexes are represented as decimal integers separated by commas. The numbers are listed in increasing order.
# # Three or more consecutive numbers are compressed and represented by the first and last element of the series, separated by a hyphen.
# # For example, if the completed indexes are 1, 3, 4, 5 and 7, they are represented as "1,3-5,7".
# def parse_completed_indexes(completed_indexes, parallelism):
#     if completed_indexes is None:
#         return []
#     indexes = []
#     index_groups = completed_indexes.split(",")
#     for index_group in index_groups:
#         if "-" in index_group:
#             r1, r2 = index_group.split("-")
#             indexes.extend(range(int(r1),int(r2)+1))
#         else:
#             indexes.append(int(index_group))
#     return indexes


def get_k8s_batch_client():
    """returns an API client supporting k8s batch API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md"""
    config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    return batch_v1


def get_k8s_core_client():
    """returns an API client support k8s core API
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md"""
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    return core_v1


def create_resources(requests=None, limits=None):
    """Creates resource limits for k8s pods
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ResourceRequirements.md"""
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
    labels=None,
    uid=None,
    backoff_limit=1,
):
    """Creates a job object for redun job.
    https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    Also creates necessary sub-objects"""

    container = client.V1Container(name=name, image=image, command=command)

    if resources is None:
        container.resources = create_resources()
    else:
        container.resources = resources
    pod_spec = client.V1PodSpec(restart_policy="Never", containers=[container])

    # Create and configurate a pod template spec section
    template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(), spec=pod_spec)

    # Create the spec of job deployment
    spec = client.V1JobSpec(template=template, backoff_limit=backoff_limit)

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


def create_job(api_instance, job, namespace):
    """Create an actual k8s job"""
    api_response = api_instance.create_namespaced_job(body=job, namespace=namespace)
    return api_response


def delete_job(api_instance, name, namespace):
    """Delete an existing k8s job"""
    body = client.V1DeleteOptions()
    api_response = api_instance.delete_namespaced_job(name=name, namespace=namespace, body=body)
    return api_response
