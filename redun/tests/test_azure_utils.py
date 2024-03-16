import os
from unittest.mock import MagicMock, patch

import pytest

from redun import azure_utils


@patch("redun.azure_utils.ManagedIdentityCredential")
@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
def test_azure_default_credential(
    default_credential_mock, on_behalf_credential_mock, mi_credential_mock
):
    cr = azure_utils.get_az_credential()
    assert cr == default_credential_mock.return_value
    on_behalf_credential_mock.assert_not_called()
    mi_credential_mock.assert_not_called()


@patch("redun.azure_utils.ManagedIdentityCredential")
@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
def test_azure_aml_fallback_credential(
    default_credential_mock, on_behalf_credential_mock, mi_credential_mock
):
    os.environ["AZUREML_WORKSPACE_ID"] = "test"
    default_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    cr = azure_utils.get_az_credential()
    del os.environ["AZUREML_WORKSPACE_ID"]
    assert cr == on_behalf_credential_mock.return_value

    default_credential_mock.assert_called_once()
    mi_credential_mock.assert_not_called()


@patch("redun.azure_utils.ManagedIdentityCredential")
@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
def test_azure_default_identity_fallback_credential(
    default_credential_mock, on_behalf_credential_mock, mi_credential_mock
):
    os.environ["AZUREML_WORKSPACE_ID"] = "test"
    os.environ["DEFAULT_IDENTITY_CLIENT_ID"] = "123-456-789"
    default_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    cr = azure_utils.get_az_credential()
    del os.environ["AZUREML_WORKSPACE_ID"]
    del os.environ["DEFAULT_IDENTITY_CLIENT_ID"]
    assert cr == mi_credential_mock.return_value
    mi_credential_mock.assert_called_once_with(client_id="123-456-789")

    on_behalf_credential_mock.assert_called_once()
    default_credential_mock.assert_called_once()


@patch("redun.azure_utils.ManagedIdentityCredential")
@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
def test_azure_fallback_credential_called_outside_aml(
    default_credential_mock, on_behalf_credential_mock, mi_credential_mock
):
    os.environ["DEFAULT_IDENTITY_CLIENT_ID"] = "123-456-789"
    default_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    cr = azure_utils.get_az_credential()
    del os.environ["DEFAULT_IDENTITY_CLIENT_ID"]
    assert cr == mi_credential_mock.return_value
    mi_credential_mock.assert_called_once_with(client_id="123-456-789")

    on_behalf_credential_mock.assert_not_called()
    default_credential_mock.assert_called_once()


@patch("redun.azure_utils.ManagedIdentityCredential")
@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
def test_azure_fallback_credentials_not_called_with_no_env(
    default_credential_mock, on_behalf_credential_mock, mi_credential_mock
):
    default_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )

    with pytest.raises(ValueError):
        _ = azure_utils.get_az_credential()

    default_credential_mock.assert_called_once()
    on_behalf_credential_mock.assert_not_called()
    mi_credential_mock.assert_not_called()


@patch("redun.azure_utils.ManagedIdentityCredential")
@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
def test_azure_raise_error_for_aml_no_managed_identity(
    default_credential_mock, on_behalf_credential_mock, mi_credential_mock
):
    """
    This test checks if in case of error getting AML credential
    and no default managed identity, the error is properly raised.
    """
    default_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )

    os.environ["AZUREML_WORKSPACE_ID"] = "test"

    with pytest.raises(ValueError):
        _ = azure_utils.get_az_credential()

    del os.environ["AZUREML_WORKSPACE_ID"]

    default_credential_mock.assert_called_once()
    on_behalf_credential_mock.assert_called_once()
    mi_credential_mock.assert_not_called()
