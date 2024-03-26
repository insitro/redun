import os
from unittest.mock import MagicMock, patch

import pytest

from redun import azure_utils


@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
@patch("redun.azure_utils.is_azure_managed_node", return_value=False)
def test_azure_default_credential_outside_azure(
    _, default_credential_mock, on_behalf_credential_mock
):
    cr = azure_utils.get_az_credential()
    default_credential_mock.assert_called_with(exclude_managed_identity_credential=True)
    assert cr == default_credential_mock.return_value
    on_behalf_credential_mock.assert_not_called()


@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
@patch("redun.azure_utils.is_azure_managed_node", return_value=True)
def test_azure_aml_credential_inside_azure(_, default_credential_mock, on_behalf_credential_mock):
    cr = azure_utils.get_az_credential()
    assert cr == on_behalf_credential_mock.return_value
    default_credential_mock.assert_not_called()


@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
@patch("redun.azure_utils.is_azure_managed_node", return_value=True)
def test_azure_default_credential_inside_azure(
    _, default_credential_mock, on_behalf_credential_mock
):
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )

    cr = azure_utils.get_az_credential()
    assert cr == default_credential_mock.return_value
    on_behalf_credential_mock.assert_called_once()


@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
@patch("redun.azure_utils.is_azure_managed_node", return_value=True)
def test_azure_default_identity_fallback_credential(
    _, default_credential_mock, on_behalf_credential_mock
):
    os.environ["DEFAULT_IDENTITY_CLIENT_ID"] = "123-456-789"
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    cr = azure_utils.get_az_credential()
    del os.environ["DEFAULT_IDENTITY_CLIENT_ID"]
    assert cr == default_credential_mock.return_value
    default_credential_mock.assert_called_once_with(managed_identity_client_id="123-456-789")

    on_behalf_credential_mock.assert_called_once()


@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
@patch("redun.azure_utils.is_azure_managed_node", return_value=True)
def test_azure_aml_job_identity_used_before_default_identity(
    _, default_credential_mock, on_behalf_credential_mock
):
    os.environ["AZUREML_USER_MANAGED_CLIENT_ID"] = "0000-0000"
    os.environ["DEFAULT_IDENTITY_CLIENT_ID"] = "123-456-789"
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    cr = azure_utils.get_az_credential()
    del os.environ["DEFAULT_IDENTITY_CLIENT_ID"]
    del os.environ["AZUREML_USER_MANAGED_CLIENT_ID"]
    assert cr == default_credential_mock.return_value
    default_credential_mock.assert_called_once_with(managed_identity_client_id="0000-0000")

    on_behalf_credential_mock.assert_called_once()


@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
@patch("redun.azure_utils.is_azure_managed_node", return_value=True)
def test_azure_client_id_variable_takes_precedence(
    _, default_credential_mock, on_behalf_credential_mock
):
    os.environ["AZURE_CLIENT_ID"] = "111-111"
    os.environ["AZUREML_USER_MANAGED_CLIENT_ID"] = "0000-0000"
    os.environ["DEFAULT_IDENTITY_CLIENT_ID"] = "123-456-789"
    on_behalf_credential_mock.return_value.get_token = MagicMock(
        side_effect=Exception("failed to get token")
    )
    cr = azure_utils.get_az_credential()
    del os.environ["DEFAULT_IDENTITY_CLIENT_ID"]
    del os.environ["AZUREML_USER_MANAGED_CLIENT_ID"]
    del os.environ["AZURE_CLIENT_ID"]
    assert cr == default_credential_mock.return_value
    default_credential_mock.assert_called_once_with(managed_identity_client_id="111-111")

    on_behalf_credential_mock.assert_called_once()


@patch("redun.azure_utils.AzureMLOnBehalfOfCredential")
@patch("redun.azure_utils.DefaultAzureCredential")
@patch("redun.azure_utils.is_azure_managed_node", return_value=True)
def test_azure_raise_error_if_all_credentials_fail_to_get_token(
    _, default_credential_mock, on_behalf_credential_mock
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

    with pytest.raises(Exception):
        _ = azure_utils.get_az_credential()

    default_credential_mock.assert_called_once()
    on_behalf_credential_mock.assert_called_once()
