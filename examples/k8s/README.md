# Kubernetes executor

redun supports running jobs on a kubernetes (k8s) cluster using the `k8s` executor. Similar to the AWS Batch Executor, you can define a new executor in the `redun.ini` config file using options like this. See the documentation for the full set of options.

```ini
[executors.k8s]
type = k8s
namespace = default
image = YOUR_AWS_ACCOUNT.dkr.ecr.YOUR_AWS_REGION.amazonaws.com/redun_example_k8s
scratch = s3://YOUR_BUCKET/redun/
min_array_size = 100
memory = 0.5

# Secrets.
secret_name = redun-secret
import_aws_secrets = True
```

In this example, we use [minikube](https://minikube.sigs.k8s.io/docs/) to provice a simple demo the k8s executor. We also show how to use AWS resources for providing a Docker Registry (ECR) and object storage scratch space (S3), however you are free to configure your own cloud-specific resources (e.g. Docker Hub, GCS).


## Getting started

To get started with this example, install and configure a local Kubernetes cluster.

On Mac, you can install Kubernetes with homebrew:

```sh
brew install kubernetes
```

A simple local cluster can be started with the command:

```sh
minikube start
```

Now we need to give our Kubernetes cluster permission to pull Docker images from ECR. If you are using your own Docker Registry, adjust the secret creation commands below.

```sh
ACCOUNT=$(aws ecr describe-registry --query registryId --output text)
REGION=$(aws configure get region)
REGISTRY=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REGISTRY

kubectl delete secret regcred
kubectl create secret docker-registry regcred \
  --docker-server=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com \
  --docker-username=AWS \
  --docker-password=$(aws ecr get-login-password)
```

Edit the redun configuration `.redun/redun.ini` to use your AWS resources by updating the following placeholders:

- YOUR_AWS_ACCOUNT
- YOUR_AWS_REGION
- YOUR_BUCKET

Like the other examples using docker images, we have provided a `Makefile` and `Dockerfile` for the example. You can build and push the docker image to ECR (or the Registry of your choice) using these commands:

```sh
cd docker/
make setup
make login
make build

# If the docker repo does not exist yet.
make create-repo

# Push the locally built image to ECR.
make push
```

## Running the example

To run a simple example workflow that launches jobs on a k8s cluster, use the command:

```sh
redun run workflow.py main
```

While the workflow is running, you should be able to see k8s jobs and pods using the commands:

```sh
kubectl get jobs
kubectl get pods
```
