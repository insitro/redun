# Kubernetes executor

## Getting started

To get started install and configure a local Kubernetes cluster.

On Mac, you can install Kubernetes with homebrew:

```sh
brew install kubernetes
```

The cluster can be started with the command:

```sh
minikube start
```

Now we need to give out Kubernetes cluster permission to pull Docker images from ECR:

```sh
ACCOUNT=$(aws ecr describe-registry --query registryId --output text)
REGION=$(aws configure get region)
REGISTRY=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com

kubectl create secret docker-registry regcred \
  --docker-server=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com \
  --docker-username=AWS \
  --docker-password=$(aws ecr get-login-password)
```
