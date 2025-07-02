# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
import datetime
import json
import time
from unittest.mock import Mock

import pytest
from azure.core.exceptions import ClientAuthenticationError

from fabric_cicd import constants
from fabric_cicd._common._exceptions import InvokeError, TokenError
from fabric_cicd._common._fabric_endpoint import FabricEndpoint, _decode_jwt, _format_invoke_log, _handle_response


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    def debug(self, message):
        self.messages.append(message)


class DummyCredential:
    def __init__(self, token):
        self.token = token
        self.raise_exception = None

    def get_token(self, *_, **__):
        if self.raise_exception:
            raise self.raise_exception
        return Mock(token=self.token)


@pytest.fixture
def setup_mocks(monkeypatch, mocker):
    dl = DummyLogger()
    mock_logger = mocker.Mock()
    mock_logger.isEnabledFor.return_value = True
    mock_logger.info.side_effect = dl.info
    mock_logger.debug.side_effect = dl.debug
    monkeypatch.setattr("fabric_cicd._common._fabric_endpoint.logger", mock_logger)
    mock_requests = mocker.patch("requests.request")
    return dl, mock_requests


def generate_mock_jwt(authtype=""):
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode().strip("=")
    payload = (
        base64.urlsafe_b64encode(json.dumps({authtype: f"{authtype}Example", "exp": 9999999999}).encode())
        .decode()
        .strip("=")
    )
    signature = "signature"
    return f"{header}.{payload}.{signature}"


def test_integration(setup_mocks):
    """Test integration of FabricEndpoint for GET request."""
    dl, mock_requests = setup_mocks
    mock_requests.return_value = Mock(
        status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke("GET", "http://example.com")
    assert response["status_code"] == 200


def test_performance(setup_mocks):
    """Test that _handle_response completes quickly under long-running simulation."""
    dl, mock_requests = setup_mocks
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


@pytest.mark.parametrize(
    ("method", "url", "body", "files"),
    [
        ("GET", "http://example.com", "{}", None),
        ("POST", "http://example.com", "{}", {"file": "test.txt"}),
    ],
    ids=["invoke", "invoke_with_files"],
)
def test_invoke(setup_mocks, method, url, body, files):
    """Test FabricEndpoint invoke method success + with optional files."""
    dl, mock_requests = setup_mocks
    mock_requests.return_value = Mock(
        status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke(method, url, body, files)
    assert response["status_code"] == 200


def test_invoke_token_expired(setup_mocks, monkeypatch):
    """Test invoking endpoint when the AAD token is expired and refreshed."""
    dl, mock_requests = setup_mocks
    mock_requests.side_effect = [
        Mock(status_code=401, headers={"x-ms-public-api-error-code": "TokenExpired"}),
        Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})),
    ]
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    endpoint.aad_token_expiration = datetime.datetime.utcnow() - datetime.timedelta(seconds=1)
    endpoint._refresh_token = Mock()
    monkeypatch.setattr("fabric_cicd._common._fabric_endpoint._format_invoke_log", lambda *_, **__: "")

    response = endpoint.invoke("GET", "http://example.com")

    assert f"{constants.INDENT}AAD token expired. Refreshing token." in dl.messages
    assert response["status_code"] == 200


def test_invoke_exception(setup_mocks):
    """Test invoking endpoint when the AAD token is expired and refreshed."""
    dl, mock_requests = setup_mocks
    mock_requests.side_effect = Exception("Test exception")
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    with pytest.raises(InvokeError):
        endpoint.invoke("GET", "http://example.com")


@pytest.mark.parametrize(
    ("auth_type", "expected_msg", "expected_upn_auth"),
    [
        ("upn", "Executing as User 'upnExample'", True),
        ("appid", "Executing as Application Id 'appidExample'", False),
        ("oid", "Executing as Object Id 'oidExample'", False),
    ],
    ids=["upn", "appid", "oid"],
)
def test_refresh_token(setup_mocks, auth_type, expected_msg, expected_upn_auth):
    """Test refreshing token and setting correct identity."""
    dl, mock_requests = setup_mocks
    jwt_token = generate_mock_jwt(authtype=auth_type)
    mock_requests.return_value = Mock(
        status_code=200,
        json=Mock(return_value={"access_token": jwt_token, "expires_in": 3600}),
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = jwt_token
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    endpoint._refresh_token()
    assert dl.messages == [expected_msg]
    assert endpoint.aad_token == jwt_token
    assert endpoint.upn_auth is expected_upn_auth


@pytest.mark.parametrize(
    ("raise_exception", "expected_msg"),
    [
        (ClientAuthenticationError("Auth failed"), "Failed to acquire AAD token. Auth failed"),
        (Exception("Unexpected error"), "An unexpected error occurred when generating the AAD token. Unexpected error"),
    ],
    ids=["auth_error", "unexpected_exception"],
)
def test_refresh_token_exceptions(raise_exception, expected_msg):
    """Test token refresh exception handling for authentication failures."""
    credential = DummyCredential("irrelevant")
    credential.raise_exception = raise_exception
    with pytest.raises(TokenError, match=expected_msg):
        FabricEndpoint(token_credential=credential)


def test_refresh_token_no_exp_claim(monkeypatch):
    """Test token refresh raising TokenError when token lacks expiration."""
    test_token = "dummy_token_value"
    credential = DummyCredential(test_token)
    monkeypatch.setattr("fabric_cicd._common._fabric_endpoint._decode_jwt", lambda _: {"upn": "user@example.com"})
    with pytest.raises(TokenError, match="Token does not contain expiration claim."):
        FabricEndpoint(token_credential=credential)


@pytest.mark.parametrize(
    (
        "status_code",
        "request_method",
        "expected_long_running",
        "expected_exit_loop",
        "input_long_running",
        "input_iteration_count",
        "response_header",
        "response_json",
    ),
    [
        (200, "POST", False, True, False, 1, {}, {}),
        (202, "POST", True, False, False, 1, {"Retry-After": 20, "Location": "new"}, {}),
        (200, "GET", True, False, True, 2, {"Retry-After": 20, "Location": "old"}, {"status": "Running"}),
        (200, "GET", False, True, True, 2, {}, {"status": "Succeeded"}),
        (200, "GET", False, False, True, 2, {"Retry-After": 20, "Location": "old"}, {"status": "Succeeded"}),
    ],
    ids=[
        "success",
        "long_running_redirect",
        "long_running_running",
        "long_running_success",
        "long_running_success_with_result",
    ],
)
def test_handle_response(
    status_code,
    request_method,
    expected_long_running,
    expected_exit_loop,
    input_long_running,
    input_iteration_count,
    response_header,
    response_json,
):
    """Test _handle_response behavior for various HTTP responses and long-running operations."""
    response = Mock(status_code=status_code, headers=response_header, json=Mock(return_value=response_json))

    exit_loop, _method, url, _body, long_running = _handle_response(
        response=response,
        method=request_method,
        url="old",
        body="{}",
        long_running=input_long_running,
        iteration_count=input_iteration_count,
    )
    assert exit_loop == expected_exit_loop
    assert long_running == expected_long_running


@pytest.mark.parametrize(
    ("exception_match", "response_json"),
    [
        (
            "[Operation failed].*",
            {"status": "Failed", "error": {"errorCode": "SampleErrorCode", "message": "Sample failure message"}},
        ),
        ("[Operation is in an undefined state].*", {"status": "Undefined"}),
    ],
    ids=["failed", "undefined"],
)
def test_handle_response_longrunning_exception(exception_match, response_json):
    """Test _handle_response raises exception for longrunning failure conditions."""
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


@pytest.mark.parametrize(
    (
        "status_code",
        "input_iteration_count",
        "input_long_running",
        "response_header",
        "return_value",
        "exception_match",
    ),
    [
        (
            401,
            1,
            False,
            {"x-ms-public-api-error-code": "Unauthorized"},
            {},
            "The executing identity is not authorized to call GET on 'http://example.com'.",
        ),
        (
            400,
            1,
            False,
            {"x-ms-public-api-error-code": "PrincipalTypeNotSupported"},
            {},
            "The executing principal type is not supported to call GET on 'http://example.com'.",
        ),
        (
            400,
            1,
            False,
            {"x-ms-public-api-error-code": "PrincipalTypeNotSupported"},
            {"message": "Test Libabry is not present in the environment."},
            "Deployment attempted to remove a library that is not present in the environment. ",
        ),
        (
            500,
            1,
            False,
            {"Content-Type": "application/json"},
            {"message": "Internal Server Error"},
            "Unhandled error occurred calling GET on 'http://example.com'. Message: Internal Server Error",
        ),
        (429, 5, True, {"Retry-After": "10"}, {}, r"Maximum retry attempts \(5\) exceeded."),
    ],
    ids=[
        "unauthorized",
        "principal_type_not_supported",
        "failed_library_removal",
        "unexpected_error",
        "retry",
    ],
)
def test_handle_response_exceptions(
    status_code, input_iteration_count, input_long_running, response_header, return_value, exception_match
):
    """Test _handle_response raises appropriate exceptions based on response error codes."""
    response = Mock(status_code=status_code, headers=response_header, json=Mock(return_value=return_value))
    with pytest.raises(Exception, match=exception_match):
        _handle_response(
            response=response,
            method="GET",
            url="http://example.com",
            body="{}",
            long_running=input_long_running,
            iteration_count=input_iteration_count,
        )


def test_handle_response_feature_not_available():
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


def test_handle_response_item_display_name_already_in_use(setup_mocks):
    """Test _handle_response logs a retry message when item display name is already in use."""
    dl, mock_requests = setup_mocks
    response = Mock(status_code=400, headers={"x-ms-public-api-error-code": "ItemDisplayNameAlreadyInUse"})
    _handle_response(response, "GET", "http://example.com", "{}", False, 1)
    expected = f"{constants.INDENT}Item name is reserved. Checking again in 60 seconds (Attempt 1/5)..."
    assert dl.messages == [expected]


def test_handle_response_environment_libraries_not_found(setup_mocks):
    """Test _handle_response exits loop when environment libraries are not found (404)."""
    dl, mock_requests = setup_mocks
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


def test_decode_jwt():
    """Test _decode_jwt decodes JWT and validates expiration claim."""
    token = generate_mock_jwt()
    decoded = _decode_jwt(token)
    assert decoded["exp"] == 9999999999


def test_decode_jwt_invalid():
    """Test _decode_jwt raises TokenError on invalid JWT."""
    with pytest.raises(TokenError):
        _decode_jwt("invalid.token")


def test_format_invoke_log():
    """Test formatting of the invoke log message."""
    response = Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={}))
    log_message = _format_invoke_log(response, "GET", "http://example.com", "{}")
    assert "Method: GET" in log_message
    assert "URL: http://example.com" in log_message


def test_handle_retry_unlimited_retries(setup_mocks):
    """Test handle_retry with unlimited retries (max_retries=None)."""
    from fabric_cicd._common._fabric_endpoint import handle_retry
    
    dl, mock_requests = setup_mocks
    
    # Test with max_retries=None (unlimited)
    handle_retry(
        attempt=10,
        base_delay=1,
        max_retries=None,
        response_retry_after=2,
        prepend_message="Operation in progress."
    )
    
    # Should log without showing max attempts
    expected = f"{constants.INDENT}Operation in progress. Checking again in 2 seconds (Attempt 10)..."
    assert dl.messages == [expected]


def test_handle_retry_unlimited_retries_negative_one(setup_mocks):
    """Test handle_retry with unlimited retries (max_retries=-1)."""
    from fabric_cicd._common._fabric_endpoint import handle_retry
    
    dl, mock_requests = setup_mocks
    
    # Test with max_retries=-1 (unlimited)
    handle_retry(
        attempt=50,
        base_delay=0.5,
        max_retries=-1,
        response_retry_after=60,
        prepend_message="Long running operation."
    )
    
    # Should log without showing max attempts, and use smaller delay (min of retry_after and exponential backoff)
    expected = f"{constants.INDENT}Long running operation. Checking again in 60 seconds (Attempt 50)..."
    assert dl.messages == [expected]


def test_handle_retry_limited_retries_still_works(setup_mocks):
    """Test handle_retry with limited retries still works as before."""
    from fabric_cicd._common._fabric_endpoint import handle_retry
    
    dl, mock_requests = setup_mocks
    
    # Test with regular max_retries
    handle_retry(
        attempt=2,
        base_delay=1,
        max_retries=5,
        response_retry_after=60,
        prepend_message="API is throttled."
    )
    
    # Should log with max attempts shown
    expected = f"{constants.INDENT}API is throttled. Checking again in 4 seconds (Attempt 2/5)..."
    assert dl.messages == [expected]


def test_handle_retry_max_exceeded_still_raises(setup_mocks):
    """Test handle_retry still raises exception when max retries exceeded for limited retries."""
    from fabric_cicd._common._fabric_endpoint import handle_retry
    
    dl, mock_requests = setup_mocks
    
    # Test that max retries exceeded still raises exception for limited retries
    with pytest.raises(Exception, match="Maximum retry attempts \\(3\\) exceeded."):
        handle_retry(
            attempt=3,
            base_delay=1,
            max_retries=3,
            response_retry_after=60,
            prepend_message="Test."
        )


def test_handle_response_long_running_operation_unlimited_retries(setup_mocks):
    """Test _handle_response uses unlimited retries for long-running operations."""
    dl, mock_requests = setup_mocks
    response = Mock(
        status_code=200,
        headers={"Retry-After": "20", "Location": "http://example.com/operation"},
        json=Mock(return_value={"status": "Running"})
    )
    
    # This should not raise an exception even with high iteration_count
    # because long-running operations now use unlimited retries
    exit_loop, method, url, body, long_running = _handle_response(
        response=response,
        method="GET",
        url="http://example.com",
        body="{}",
        long_running=True,
        iteration_count=100,  # High number that would exceed old limit
    )
    
    # Should continue waiting (not exit loop) and use unlimited retries
    assert exit_loop is False
    assert long_running is True
    
    # Should log without max attempts limit
    expected = f"{constants.INDENT}{constants.INDENT}Operation in progress. Checking again in 20 seconds (Attempt 99)..."
    assert dl.messages == [expected]
