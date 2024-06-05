"""
Helper functions for Azure integration.
"""

import os

import requests

from redun.logging import logger

try:
    from azure.ai.ml.identity import AzureMLOnBehalfOfCredential
    from azure.identity import DefaultAzureCredential
except (ImportError, ModuleNotFoundError):
    pass


def is_azure_managed_node() -> bool:
    """
    Function that checks if the current compute is backed by an Azure VM.
    Returns False for local devices or VMs hosted on other clouds (EC2).
    https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service?tabs=linux
    """
    imds_url = "http://169.254.169.254/metadata/instance?api-version=2021-02-01"
    headers = {"Metadata": "true"}

    try:
        response = requests.get(imds_url, headers=headers, timeout=2)
        response.raise_for_status()
        # If the request is successful, we are likely on an Azure VM
        logger.debug("This node is an Azure-managed node.")
        return True
    except requests.RequestException:
        # If the request fails, we are likely not on an Azure VM
        logger.debug("This node is not an Azure-managed node.")
        return False


def get_az_credential():
    """
    Retrieve credential object and validate if it has permissions
    for storage operations (it doesn't validate the permissions to specific
    account, only if it can get token from storage.azure.com).

    The order of getting credentials is:
     - DefaultAzureCredential. By default, it uses AZURE_CLIENT_ID to look up managed
       identity info.
     - AzureMLOnBehalfOfCredential which is available only in AML compute
       (the presence of AZUREML_WORKSPACE_ID env variable is used to determine if we
       run in AML compute).
     - ManagedIdentityCredential if DEFAULT_IDENTITY_CLIENT_ID env variable is present.
       This is the last option in case we run a task in a compute cluster with default
       assigned identity and we want to override it.
    """

    if is_azure_managed_node():
        mi_client_id = (
            # the default name of the env variable, used by most tools
            os.environ.get("AZURE_CLIENT_ID", None)
            or
            # passed to AML job via ManagedIdentityConfiguration
            os.environ.get("AZUREML_USER_MANAGED_CLIENT_ID", None)
            or
            # available in AML clusters with user-assigned MI
            os.environ.get("DEFAULT_IDENTITY_CLIENT_ID", None)
        )

        # try AzureMLOnBehalfOfCredential - it doesn't use exception classes
        # from azure.identity package, so it's not compatible with ChainedTokenCredentials
        # and we need to check beforehand if it's able to get any tokens
        cr = AzureMLOnBehalfOfCredential()
        try:
            cr.get_token("https://storage.azure.com")
        except:  # noqa: E722
            cr = DefaultAzureCredential(managed_identity_client_id=mi_client_id)
    else:
        # don't attempt MI or AML credentials outside Azure
        cr = DefaultAzureCredential(exclude_managed_identity_credential=True)

    # validate if the credential can get tokens and fail early if not
    cr.get_token("https://storage.azure.com")

    return cr
