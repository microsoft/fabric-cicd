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
        base64
        .urlsafe_b64encode(json.dumps({authtype: f"{authtype}Example", "exp": 9999999999}).encode())
        .decode()
        .strip("=")
    )
    signature = "signature"
    return f"{header}.{payload}.{signature}"


def test_integration(setup_mocks):
    """Test integration of FabricEndpoint for GET request."""
    _, mock_requests = setup_mocks
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
    _, _mock_requests = setup_mocks
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
    _, mock_requests = setup_mocks
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
    _, mock_requests = setup_mocks
    mock_requests.side_effect = Exception("Test exception")
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    with pytest.raises(InvokeError):
        endpoint.invoke("GET", "http://example.com")


def test_invoke_poll_long_running_false_with_202(setup_mocks):
    """Test invoke method with poll_long_running=False exits early on 202 response."""
    _, mock_requests = setup_mocks
    mock_requests.return_value = Mock(
        status_code=202,
        headers={"Content-Type": "application/json", "Location": "http://example.com/status"},
        json=Mock(return_value={}),
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    response = endpoint.invoke("POST", "http://example.com", poll_long_running=False)

    # Should exit immediately without polling
    assert response["status_code"] == 202
    assert mock_requests.call_count == 1  # Only one request, no polling


def test_invoke_poll_long_running_true_with_202(setup_mocks, monkeypatch):
    """Test invoke method with poll_long_running=True polls on 202 response."""
    _, mock_requests = setup_mocks

    # First call returns 202 with Location header, second call returns 200 with Succeeded status
    mock_requests.side_effect = [
        Mock(
            status_code=202,
            headers={"Content-Type": "application/json", "Location": "http://example.com/status"},
            json=Mock(return_value={}),
            text="{}",
        ),
        Mock(
            status_code=200,
            headers={"Content-Type": "application/json"},
            json=Mock(return_value={"status": "Succeeded"}),
            text='{"status": "Succeeded"}',
        ),
    ]

    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    # Mock time.sleep to avoid delays in tests
    monkeypatch.setattr("time.sleep", lambda _: None)

    response = endpoint.invoke("POST", "http://example.com", poll_long_running=True)

    # Should poll and return final status
    assert response["status_code"] == 200
    assert mock_requests.call_count == 2  # Initial request + polling request


def test_invoke_poll_long_running_default_with_202(setup_mocks, monkeypatch):
    """Test invoke method with default poll_long_running (True) polls on 202 response."""
    _, mock_requests = setup_mocks

    # First call returns 202 with Location header, second call returns 200 with Succeeded status
    mock_requests.side_effect = [
        Mock(
            status_code=202,
            headers={"Content-Type": "application/json", "Location": "http://example.com/status"},
            json=Mock(return_value={}),
            text="{}",
        ),
        Mock(
            status_code=200,
            headers={"Content-Type": "application/json"},
            json=Mock(return_value={"status": "Succeeded"}),
            text='{"status": "Succeeded"}',
        ),
    ]

    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value.token = generate_mock_jwt()
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    # Mock time.sleep to avoid delays in tests
    monkeypatch.setattr("time.sleep", lambda _: None)

    # Don't pass poll_long_running, should default to True
    response = endpoint.invoke("POST", "http://example.com")

    # Should poll and return final status
    assert response["status_code"] == 200
    assert mock_requests.call_count == 2  # Initial request + polling request


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
    with pytest.raises(TokenError, match=r"Token does not contain expiration claim."):
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

    exit_loop, _method, _url, _body, long_running = _handle_response(
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
        "max_duration",
        "start_time",
    ),
    [
        (
            401,
            1,
            False,
            {"x-ms-public-api-error-code": "Unauthorized"},
            {},
            "The executing identity is not authorized to call GET on 'http://example.com'.",
            None,
            None,
        ),
        (
            400,
            1,
            False,
            {"x-ms-public-api-error-code": "PrincipalTypeNotSupported"},
            {},
            "The executing principal type is not supported to call GET on 'http://example.com'.",
            None,
            None,
        ),
        (
            400,
            1,
            False,
            {"x-ms-public-api-error-code": "PrincipalTypeNotSupported"},
            {"message": "Test Libabry is not present in the environment."},
            "Deployment attempted to remove a library that is not present in the environment. ",
            None,
            None,
        ),
        (
            500,
            1,
            False,
            {"Content-Type": "application/json"},
            {"message": "Internal Server Error"},
            "Unhandled error occurred calling GET on 'http://example.com'. Message: Internal Server Error",
            None,
            None,
        ),
        (429, 5, True, {"Retry-After": "10"}, {}, r"Maximum execution duration \(0 seconds\) exceeded", 0, 0.0),
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
    status_code, input_iteration_count, input_long_running, response_header, return_value, exception_match, max_duration, start_time
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
            max_duration=max_duration,
            start_time=start_time,
        )


def test_handle_response_feature_not_available():
    """Test _handle_response for feature not available"""
    response = Mock(status_code=403, reason="FeatureNotAvailable")
    with pytest.raises(Exception, match=r"Item type not supported. Description: FeatureNotAvailable"):
        _handle_response(
            response=response,
            method="GET",
            url="http://example.com",
            body="{}",
            long_running=False,
            iteration_count=1,
        )


def test_handle_response_item_display_name_already_in_use(setup_mocks, monkeypatch):
    """
    Test _handle_response logs a retry message when item display name is already in use.
    
    Mocks time.sleep to avoid actual test execution delays.
    """
    import time
    dl, _mock_requests = setup_mocks    
    monkeypatch.setattr("time.sleep", lambda _: None)
    response = Mock(status_code=400, headers={"x-ms-public-api-error-code": "ItemDisplayNameNotAvailableYet"})
    _handle_response(response, "GET", "http://example.com", "{}", False, 1, max_duration=300, start_time=time.time())
    expected = f"{constants.INDENT}Item name is reserved. Checking again in 60 seconds (Attempt 1)..."
    assert dl.messages == [expected]


def test_handle_response_environment_libraries_not_found(setup_mocks):
    """Test _handle_response exits loop when environment libraries are not found (404)."""
    _, _mock_requests = setup_mocks
    response = Mock(status_code=404, headers={"x-ms-public-api-error-code": "EnvironmentLibrariesNotFound"})
    exit_loop, _method, _url, _body, long_running = _handle_response(
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


def test_is_fabric_runtime_returns_false():
    """Test _is_fabric_runtime returns False when not running in Fabric Notebook environment."""
    from fabric_cicd._common._fabric_endpoint import _is_fabric_runtime

    # When running tests, we're not in a Fabric Runtime environment
    # so _is_fabric_runtime should return False
    assert _is_fabric_runtime() is False


def test_generate_fabric_credential(monkeypatch):
    """Test _generate_fabric_credential returns a TokenCredential that can get tokens."""
    import builtins

    from fabric_cicd._common._fabric_endpoint import _generate_fabric_credential

    # Mock notebookutils and get_ipython
    mock_notebookutils = Mock()
    mock_notebookutils.credentials.getToken.return_value = generate_mock_jwt()

    mock_ipython = Mock()
    mock_ipython.user_ns.get.return_value = mock_notebookutils

    # Patch get_ipython as a new attribute on builtins module
    monkeypatch.setattr(builtins, "get_ipython", lambda: mock_ipython, raising=False)

    # Generate the credential
    credential = _generate_fabric_credential()

    # Verify it's a TokenCredential that can get tokens
    assert credential is not None
    token = credential.get_token()

    # Verify the token has the expected structure
    assert token.token is not None
    assert token.expires_on is not None

    # Verify notebookutils.credentials.getToken was called
    mock_notebookutils.credentials.getToken.assert_called_once_with("pbi")


def test_generate_fabric_credential_fallback_expiration(monkeypatch):
    """Test _generate_fabric_credential uses fallback expiration when JWT parsing fails."""
    import builtins
    import time

    from fabric_cicd._common._fabric_endpoint import _generate_fabric_credential

    # Mock notebookutils to return an invalid JWT token
    mock_notebookutils = Mock()
    mock_notebookutils.credentials.getToken.return_value = "invalid.jwt.token"

    mock_ipython = Mock()
    mock_ipython.user_ns.get.return_value = mock_notebookutils

    # Patch get_ipython as a new attribute on builtins module
    monkeypatch.setattr(builtins, "get_ipython", lambda: mock_ipython, raising=False)

    # Generate the credential
    credential = _generate_fabric_credential()

    # Get token - should use fallback expiration (current time + 1 hour)
    current_time = int(time.time())
    token = credential.get_token()

    # Verify the token uses fallback expiration (~1 hour from now)
    assert token.expires_on >= current_time + 3500  # ~58 minutes
    assert token.expires_on <= current_time + 3700  # ~62 minutes
