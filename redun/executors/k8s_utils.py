from kubernetes import client, config

DEFAULT_JOB_PREFIX='redun-job'

def get_k8s_batch_client():
    config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    return batch_v1

def get_k8s_core_client():
    config.load_kube_config()
    core_v1 = client.CoreV1Api()
    return core_v1

def create_resources(requests=None, limits=None):
    resources = client.V1ResourceRequirements()

    if requests is None:
        resources.requests = { }
    else:
        resources.requests = requests

    if limits is None:
        resources.limits = { }
    else:
        resources.limits = limits

    return resources

def create_container(name, image, command):
    return client.V1Container(
        name=name,
        image=image,
        command=command,
    )

def create_job_object(name=DEFAULT_JOB_PREFIX, image="bash", command="false", resources=None,
    labels=None, uid=None):

    container = create_container(name, image, command)
    if resources == None:
        container.resources = create_resources()
    else:
        container.resources = resources
    pod_spec = client.V1PodSpec(restart_policy="Never", containers=[container])

    if labels is None:
        labels = {}

    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(),
        spec=pod_spec)
    # Create the specification of deployment
    spec = client.V1JobSpec(
        template=template,
        backoff_limit=1)
    # Instantiate the job object
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=name, labels=labels, uid=uid),
        spec=spec)
    return job

def create_job(api_instance, job, namespace):
    api_response = api_instance.create_namespaced_job(
        body=job,
        namespace=namespace)
    return api_response
