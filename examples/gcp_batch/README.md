# Running workflows in Google Batch

redun can run individual tasks on [Google Batch](https://cloud.google.com/batch) similar to our [previous example for AWS Batch](../05_aws_batch). Follow the instructions below to prepare redun to work with Google Batch, and run an example workflow.

## Setup

First, ensure you have the Google Cloud client installed. See [Google's documentation](https://cloud.google.com/sdk/docs/install) for more details.

Next, ensure you set your GCP project id. It can be done the command line as follows:

```sh
gcloud config set project YOUR_PROJECT_ID
```

Since we will be calling GCP API's we will need to login to GCP.

```sh
gcloud auth login
#gcloud auth application-default login
```

```sh
# Define your preferred GCP region using an environment variable.
export REGION=us-west1

cd docker
make setup

# Authenticate Docker to the Google Docker registry `pkg.dev`.
make login

# Create a new Docker repo in the registry.
make create-repo

# Build and push a redun-enabled Docker image to the registry.
make build
make push
```

Ensure you have a GCS bucket or prefix set aside for redun scratch space. A new bucket can be created using the following command:

```sh
gcloud storage buckets create gs://YOUR_BUCKET
```

## Configuring executors

Now edit the `.redun/redun.ini` file to configure redun to submit jobs to Google Batch. We will need to set our GCP project, region, GCS scratch path, and the Docker image we just made.

```ini
# redun/redun.ini
[executors.gcp_batch]
type = gcp_batch
project = YOUR_PROJECT
region = YOUR_REGION
gcs_scratch = gs://YOUR_BUCKET/redun
image = YOUR_REGION-docker.pkg.dev/YOUR_PROJECT/redun-example
```

## Run the example

Once all the setup and configuration above is complete, run the example workflow as follows:

```sh
redun run workflow.py main
```
