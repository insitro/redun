import os
import threading
from typing import Dict, Iterator, List, NamedTuple, Optional, Tuple
from urllib.error import URLError
from urllib.request import urlopen

import boto3

from redun.file import File

# Constants.
DEFAULT_AWS_REGION = "us-west-2"

# Cache for AWS Clients.
_boto_clients: Dict[Tuple[int, str, str], boto3.Session] = {}


class JobStatus(NamedTuple):
    all: List[str]
    pending: List[str]
    inflight: List[str]
    success: List[str]
    failure: List[str]
    stopped: List[str]
    timeout: List[str]


def get_aws_client(service: str, aws_region: str = DEFAULT_AWS_REGION) -> boto3.Session:
    """
    Get an AWS Client with caching.
    """
    cache_key = (threading.get_ident(), service, aws_region)
    client = _boto_clients.get(cache_key)
    if not client:
        # Boto is not thread safe, so we create a client per thread using
        # `threading.get_ident()` as part of our cache key.
        # We need to create the client using a session to avoid all clients
        # sharing the same global session.
        # See: https://github.com/boto/boto3/issues/801#issuecomment-440942144
        session = boto3.session.Session()
        client = _boto_clients[cache_key] = session.client(service, region_name=aws_region)

    return client


def is_ec2_instance() -> bool:
    """
    Returns True if this process is running on an EC2 instance.

    We use the presence of a link-local address as a sign we are on an EC2 instance.
    """
    try:
        resp = urlopen("http://169.254.169.254/latest/meta-data/", timeout=1)
        return resp.status == 200
    except URLError:
        return False


def get_aws_env_vars() -> Dict[str, str]:
    """
    Determines the current AWS credentials.
    """
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    cred_map = {
        "AWS_ACCESS_KEY_ID": creds.access_key,
        "AWS_SECRET_ACCESS_KEY": creds.secret_key,
        "AWS_SESSION_TOKEN": creds.token,
    }
    # Skip variables that are not defined.
    return {k: v for k, v in cred_map.items() if v}


def copy_to_s3(file_path: str, s3_scratch_dir: str) -> str:
    """
    Copies a file to the S3 scratch directory if it is not already on S3.
    Returns the path to the file on S3.
    """
    file = File(file_path)
    _, filename = os.path.split(file.path)

    s3_temp_file = File(f"{s3_scratch_dir.rstrip('/')}/{filename}")
    file.copy_to(s3_temp_file)
    return s3_temp_file.path


def get_default_region() -> str:
    """
    Returns the default AWS region.
    """
    return boto3.Session().region_name or DEFAULT_AWS_REGION


def get_aws_user(aws_region: str = DEFAULT_AWS_REGION) -> str:
    """
    Returns the current AWS user.
    """
    sts_client = get_aws_client("sts", aws_region=aws_region)
    response = sts_client.get_caller_identity()
    return response["Arn"]


def get_simple_aws_user(aws_region: str = DEFAULT_AWS_REGION) -> str:
    """
    Returns the current AWS user simplified.

    By full AWS identify has the format:

      arn:aws:sts::ACCOUNT_ID:USER_NAME
      arn:aws:sts::ACCOUNT_ID:assumed-role/ROLE_NAME/USER_NAME

    This function will return USER_NAME as a simplification.
    """
    user_arn_parts = get_aws_user(aws_region).split(":")
    resource_id = user_arn_parts[5]
    if "/" not in resource_id:
        return resource_id

    resource_type, username = resource_id.split("/", 1)
    if resource_type == "assumed-role":
        role_name, username = username.split("/", 1)
    return username


def iter_log_stream(
    log_group_name: str,
    log_stream: str,
    limit: Optional[int] = None,
    reverse: bool = False,
    required: bool = True,
    aws_region: str = DEFAULT_AWS_REGION,
) -> Iterator[dict]:
    """
    Iterate through the events of logStream.
    """
    logs_client = get_aws_client("logs", aws_region=aws_region)
    try:
        response = logs_client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream,
            startFromHead=not reverse,
            # boto API does not allow passing None, so we must fully exclude the parameter.
            **{"limit": limit} if limit else {},
        )
    except logs_client.exceptions.ResourceNotFoundException as error:
        if required:
            # If logs are required, raise an error.
            raise error
        else:
            return

    while True:
        events = response["events"]

        # If no events, we are at the end of the stream.
        if not events:
            break

        if reverse:
            events = reversed(events)
        yield from events

        if not reverse:
            next_token = response["nextForwardToken"]
        else:
            next_token = response["nextBackwardToken"]
        response = logs_client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream,
            nextToken=next_token,
            # boto API does not allow passing None, so we must fully exclude the parameter.
            **{"limit": limit} if limit else {},
        )
