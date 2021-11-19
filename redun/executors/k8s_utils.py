from os import path
from time import sleep

import yaml

from kubernetes import client, config

def get_k8s_client():
    config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    return batch_v1

def create_job_object(job_name, image, command):
    container = client.V1Container(
        name=job_name,
        image=image,
        command=command)
    # Create and configurate a spec section
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(),
        spec=client.V1PodSpec(restart_policy="Never", containers=[container]))
    # Create the specification of deployment
    spec = client.V1JobSpec(
        template=template,
        backoff_limit=4)
    # Instantiate the job object
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name),
        spec=spec)

    return job


def create_job(api_instance, job):
    api_response = api_instance.create_namespaced_job(
        body=job,
        namespace="default")
    print("Job created. status='%s'" % str(api_response.status))


def main():
    # Configs can be set in Configuration class directly or using helper
    # utility. If no argument provided, the config will be loaded from
    # default location.
    config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    # Create a job object with client-python API. The job we
    # created is same as the `pi-job.yaml` in the /examples folder.
    job = create_job_object("redunjob", "242314368270.dkr.ecr.us-west-2.amazonaws.com/bioformats2raw", ["/opt/bioformats2raw/bin/bioformats2raw"])

    create_job(batch_v1, job)

if __name__ == '__main__':
    main()