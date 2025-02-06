# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
import json
import time
from unittest.mock import Mock

import pytest

from fabric_cicd._common._exceptions import InvokeError, TokenError
from fabric_cicd._common._fabric_endpoint import FabricEndpoint, _decode_jwt, _format_invoke_log, _handle_response


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)


@pytest.fixture
def dummy_logger(monkeypatch):
    dl = DummyLogger()
    monkeypatch.setattr("fabric_cicd._common._fabric_endpoint.logger", dl)
    return dl


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("requests.request")


def generate_mock_jwt():
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode().strip("=")
    payload = base64.urlsafe_b64encode(json.dumps({"exp": 9999999999}).encode()).decode().strip("=")
    signature = "signature"
    return f"{header}.{payload}.{signature}"


def test_integration(mock_requests):
    """Integration test for FabricEndpoint"""
    mock_requests.return_value = Mock(
        status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke("GET", "http://example.com")
    assert response["status_code"] == 200


def test_performance(mock_requests):
    """Performance test for _handle_response"""
    response = Mock(status_code=200, headers={}, json=Mock(return_value={"status": "Succeeded"}))
    start_time = time.time()
    _handle_response(
        response=response,
        method="GET",
        url="old",
        body="{}",
        long_running=True,
        iteration_count=2,
    )
    end_time = time.time()
    assert (end_time - start_time) < 1  # Ensure the function completes within 1 second


def test_invoke(mock_requests):
    """Test the invoke method of FabricEndpoint"""
    mock_requests.return_value = Mock(
        status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke("GET", "http://example.com")
    assert response["status_code"] == 200


def test_invoke_with_files(mock_requests):
    """Test the invoke method of FabricEndpoint with files"""
    mock_requests.return_value = Mock(
        status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke("POST", "http://example.com", files={"file": "test.txt"})
    assert response["status_code"] == 200


def test_invoke_exception(mock_requests):
    """Test the invoke method of FabricEndpoint with exception"""
    mock_requests.side_effect = Exception("Test exception")
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    with pytest.raises(InvokeError):
        endpoint.invoke("GET", "http://example.com")


def test_refresh_token(mock_requests):
    """Test the _refresh_token method of FabricEndpoint"""
    mock_requests.return_value = Mock(
        status_code=200, json=Mock(return_value={"access_token": generate_mock_jwt(), "expires_in": 3600})
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    endpoint._refresh_token()
    assert endpoint.aad_token == generate_mock_jwt()


# def test_invoke_token_expired(mock_requests):
#     """Test the invoke method of FabricEndpoint with expired token"""
#     mock_requests.side_effect = [
#         Mock(status_code=401, headers={"x-ms-public-api-error-code": "TokenExpired"}),
#         Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})),
#     ]
#     mock_token_credential = Mock()
#     mock_token_credential.get_token.return_value.token = generate_mock_jwt()
#     endpoint = FabricEndpoint(token_credential=mock_token_credential)
#     response = endpoint.invoke("GET", "http://example.com")
#     assert response["status_code"] == 200


@pytest.mark.parametrize(
    ("status_code", "request_method", "expected_long_running", "expected_exit_loop", "response_header"),
    [
        (200, "POST", False, True, {}),
        (202, "POST", True, False, {"Retry-After": 20, "Location": "new"})
    ],
    ids=[
        "success",
        "long_running_redirect",
    ])  # fmt: skip
def test_handle_response(status_code, request_method, expected_long_running, expected_exit_loop, response_header):
    """Long running scenarios expected to pass"""
    response = Mock(status_code=status_code, headers=response_header, json=Mock(return_value={}))

    exit_loop, _method, url, _body, long_running = _handle_response(
        response=response,
        method=request_method,
        url="old",
        body="{}",
        long_running=False,
        iteration_count=1,
    )
    assert exit_loop == expected_exit_loop
    assert long_running == expected_long_running


@pytest.mark.parametrize(
    ("expected_long_running", "expected_exit_loop", "response_json", "response_header"),
    [
        (True, False, {"status": "Running"}, {"Retry-After": 20, "Location": "old"}),
        (False, True, {"status": "Succeeded"}, {}),
        (False, False, {"status": "Succeeded"}, {"Retry-After": 20, "Location": "old"}),
    ],
    ids=[
        "long_running_running",
        "long_running__success",
        "long_running__success_with_result",
    ])  # fmt: skip
def test_handle_response_longrunning(expected_long_running, expected_exit_loop, response_json, response_header):
    """Long running scenarios expected to pass"""
    response = Mock(status_code=200, headers=response_header, json=Mock(return_value=response_json))

    exit_loop, _method, _url, _body, long_running = _handle_response(
        response=response,
        method="GET",
        url="old",
        body="{}",
        long_running=True,
        iteration_count=2,
    )
    assert exit_loop == expected_exit_loop
    assert long_running == expected_long_running


@pytest.mark.parametrize(
    ("exception_match", "response_json"),
    [
        ("[Operation failed].*", {"status": "Failed","error": {"errorCode": "SampleErrorCode", "message": "Sample failure message"}}),
        ("[Operation is in an undefined state].*", {"status": "Undefined"}),
    ],
    ids=[
        "failed",
        "undefined",
    ])  # fmt: skip
def test_handle_response_longrunning_exception(
    exception_match,
    response_json,
):
    """Long running scenarios expected to pass"""
    response = Mock(status_code=200, headers={}, json=Mock(return_value=response_json))

    with pytest.raises(Exception, match=exception_match):
        _handle_response(
            response=response,
            method="GET",
            url="old",
            body="{}",
            long_running=True,
            iteration_count=2,
        )


def test_handle_response_environment_libraries_not_found(mock_requests):
    """Test _handle_response for environment libraries not found"""
    response = Mock(status_code=404, headers={"x-ms-public-api-error-code": "EnvironmentLibrariesNotFound"})
    exit_loop, method, url, body, long_running = _handle_response(
        response=response,
        method="GET",
        url="http://example.com",
        body="{}",
        long_running=False,
        iteration_count=1,
    )
    assert exit_loop is True
    assert long_running is False


def test_handle_response_throttled(dummy_logger):
    """Test _handle_response for API throttling"""
    response = Mock(status_code=429, headers={"Retry-After": "10"})
    _handle_response(response, "GET", "http://example.com", "{}", False, 1)
    expected = "API is throttled. Checking again in 10 seconds (Attempt 1/5)..."
    assert dummy_logger.messages == [expected]


def test_handle_response_unauthorized(mock_requests):
    """Test _handle_response for unauthorized access"""
    response = Mock(status_code=401, headers={"x-ms-public-api-error-code": "Unauthorized"})
    with pytest.raises(
        Exception, match="The executing identity is not authorized to call GET on 'http://example.com'."
    ):
        _handle_response(
            response=response,
            method="GET",
            url="http://example.com",
            body="{}",
            long_running=False,
            iteration_count=1,
        )


def test_handle_response_item_display_name_already_in_use(dummy_logger):
    """Test _handle_response for item display name already in use"""
    response = Mock(status_code=400, headers={"x-ms-public-api-error-code": "ItemDisplayNameAlreadyInUse"})
    _handle_response(response, "GET", "http://example.com", "{}", False, 1)
    expected = "Item name is reserved. Checking again in 5 seconds (Attempt 1/5)..."
    assert dummy_logger.messages == [expected]


def test_handle_response_failed_library_removal(mock_requests):
    """Test _handle_response for principal type not supported"""
    response = Mock(
        status_code=400,
        headers={"x-ms-public-api-error-code": "PrincipalTypeNotSupported"},
        json=Mock(return_value={"message": "Test Libabry is not present in the environment."}),
    )
    with pytest.raises(
        Exception, match="Deployment attempted to remove a library that is not present in the environment. "
    ):
        _handle_response(
            response=response,
            method="GET",
            url="http://example.com",
            body="{}",
            long_running=False,
            iteration_count=1,
        )


def test_handle_response_principal_type_not_supported(mock_requests):
    """Test _handle_response for principal type not supported"""
    response = Mock(
        status_code=400, headers={"x-ms-public-api-error-code": "PrincipalTypeNotSupported"}, json=Mock(return_value={})
    )
    with pytest.raises(
        Exception, match="The executing principal type is not supported to call GET on 'http://example.com'."
    ):
        _handle_response(
            response=response,
            method="GET",
            url="http://example.com",
            body="{}",
            long_running=False,
            iteration_count=1,
        )


def test_handle_response_feature_not_available(mock_requests):
    """Test _handle_response for feature not available"""
    response = Mock(status_code=403, reason="FeatureNotAvailable")
    with pytest.raises(Exception, match="Item type not supported. Description: FeatureNotAvailable"):
        _handle_response(
            response=response,
            method="GET",
            url="http://example.com",
            body="{}",
            long_running=False,
            iteration_count=1,
        )


def test_handle_response_max_retry(mock_requests):
    """Test _handle_response for retry"""
    response = Mock(status_code=429, headers={"Retry-After": "10"})
    with pytest.raises(Exception, match=r"Maximum retry attempts \(5\) exceeded."):
        _handle_response(
            response=response,
            method="GET",
            url="http://example.com",
            body="{}",
            long_running=True,
            iteration_count=5,
        )


def test_decode_jwt():
    """Test _decode_jwt function"""
    token = generate_mock_jwt()
    decoded = _decode_jwt(token)
    assert decoded["exp"] == 9999999999


def test_decode_jwt_invalid():
    """Test _decode_jwt function with invalid token"""
    with pytest.raises(TokenError):
        _decode_jwt("invalid.token")


def test_format_invoke_log():
    """Test _format_invoke_log function"""
    response = Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={}))
    log_message = _format_invoke_log(response, "GET", "http://example.com", "{}")
    assert "Method: GET" in log_message
    assert "URL: http://example.com" in log_message
