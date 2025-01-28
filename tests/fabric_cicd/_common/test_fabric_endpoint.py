from unittest.mock import Mock, patch

import pytest

from fabric_cicd._common._fabric_endpoint import FabricEndpoint


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("requests.request")


@pytest.fixture
def fabric_endpoint():
    mock_credential = Mock()
    with patch.object(FabricEndpoint, "_refresh_token", return_value="mocked_token"):
        return FabricEndpoint(token_credential=mock_credential)


def test_invoke_success(mock_requests, fabric_endpoint):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {"key": "value"}
    mock_requests.return_value = mock_response

    result = fabric_endpoint.invoke("GET", "http://example.com")

    assert result["status_code"] == 200
    assert result["body"] == {"key": "value"}


def test_invoke_throttle(mock_requests, fabric_endpoint):
    mock_response = Mock()
    mock_response.status_code = 429
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response.json.return_value = {"key": "value"}
    mock_requests.return_value = mock_response

    result = fabric_endpoint.invoke("GET", "http://example.com")

    assert result["status_code"] == 429
    assert result["body"] == {"key": "value"}
