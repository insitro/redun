"""
Helper functions for Azure integration.
"""
import os

from redun.logging import logger

try:
    from azure.ai.ml.identity import AzureMLOnBehalfOfCredential
    from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
except (ImportError, ModuleNotFoundError):
    pass


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

    try:
        cr = DefaultAzureCredential()
        # quick validation
        cr.get_token("https://storage.azure.com/")
        return cr
    except:  # noqa: E722
        # special credential available only inside AML compute
        # when running with "user_identity"
        # https://learn.microsoft.com/en-us/samples/azure/azureml-examples/azureml---on-behalf-of-feature/
        if "AZUREML_WORKSPACE_ID" in os.environ:
            try:
                logger.info(
                    "Failed to create DefaultAzureCredential, trying AzureMLOnBehalfOfCredential."
                )
                cr = AzureMLOnBehalfOfCredential()
                cr.get_token("https://storage.azure.com/")
                return cr
            except:  # noqa: E722
                pass
        if "DEFAULT_IDENTITY_CLIENT_ID" in os.environ:
            logger.info("Trying ManagedIdentityCredential with DEFAULT_IDENTITY_CLIENT_ID.")
            cr = ManagedIdentityCredential(client_id=os.environ["DEFAULT_IDENTITY_CLIENT_ID"])
            cr.get_token("https://storage.azure.com/")
            return cr

    raise ValueError("Failed to create Azure credential.")
