# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import time
from unittest.mock import Mock

import pytest
import requests
from azure.core.exceptions import ClientAuthenticationError

from fabric_cicd import constants
from fabric_cicd._common._exceptions import InvokeError, TokenError
from fabric_cicd._common._fabric_endpoint import FabricEndpoint, _format_invoke_log, _handle_response, handle_retry


class DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    def debug(self, message):
        self.messages.append(message)


class DummyCredential:
    def __init__(self, token, expires_on=9999999999):
        self.token = token
        self.expires_on = expires_on
        self.raise_exception = None

    def get_token(self, *_, **__):
        if self.raise_exception:
            raise self.raise_exception
        return Mock(token=self.token, expires_on=self.expires_on)


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


def generate_mock_token():
    return "mock_token_value"


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
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke(method, url, body, files)
    assert response["status_code"] == 200


def test_invoke_token_expired(setup_mocks, monkeypatch):
    """Test invoking endpoint when the Microsoft Entra token is expired and refreshed."""
    dl, mock_requests = setup_mocks
    mock_requests.side_effect = [
        Mock(status_code=401, headers={"x-ms-public-api-error-code": "TokenExpired"}),
        Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})),
    ]
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    monkeypatch.setattr("fabric_cicd._common._fabric_endpoint._format_invoke_log", lambda *_, **__: "")

    response = endpoint.invoke("GET", "http://example.com")

    assert f"{constants.INDENT}Microsoft Entra token expired. Retrying with refreshed token." in dl.messages
    assert response["status_code"] == 200

    # Assert get_token was called: once at init (cached for first request) + once for retry after invalidation
    assert mock_token_credential.get_token.call_count == 2


def test_get_token_proactive_refresh_on_expiry(setup_mocks):
    """Test that _get_token fetches a new token when cached token has expired by time."""
    _, mock_requests = setup_mocks
    mock_requests.return_value = Mock(
        status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})
    )
    mock_token_credential = Mock()
    # First token expires immediately (epoch 0), second token is long-lived
    mock_token_credential.get_token.side_effect = [
        Mock(token="first_token", expires_on=0),
        Mock(token="second_token", expires_on=9999999999),
    ]
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    # Init consumed first token (already expired), so next call should fetch again
    token = endpoint._get_token()

    assert token == "second_token"
    assert mock_token_credential.get_token.call_count == 2


def test_get_token_refreshes_within_expiry_buffer(setup_mocks):
    """Test that _get_token refreshes when token expires within the 10-second buffer."""
    _, _mock_requests = setup_mocks
    mock_token_credential = Mock()
    # First token expires in 5 seconds (within buffer), second token is long-lived
    expires_within_buffer = time.time() + 5
    mock_token_credential.get_token.side_effect = [
        Mock(token="near_expiry_token", expires_on=expires_within_buffer),
        Mock(token="fresh_token", expires_on=9999999999),
    ]
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    # Token expires in 5s which is within the 10s buffer, so it should refresh
    token = endpoint._get_token()

    assert token == "fresh_token"
    assert mock_token_credential.get_token.call_count == 2


def test_get_token_caches_when_outside_expiry_buffer(setup_mocks):
    """Test that _get_token returns cached token when expiry is beyond the 10-second buffer."""
    _, _mock_requests = setup_mocks
    mock_token_credential = Mock()
    # Token expires in 60 seconds (well beyond buffer)
    expires_outside_buffer = time.time() + 60
    mock_token_credential.get_token.return_value = Mock(token="valid_token", expires_on=expires_outside_buffer)
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    # Token expires in 60s which is beyond the 10s buffer, so it should use cache
    token = endpoint._get_token()

    assert token == "valid_token"
    # Only called once at init — subsequent call returns cached
    assert mock_token_credential.get_token.call_count == 1


def test_invoke_sends_refreshed_token_in_header(setup_mocks, monkeypatch):
    """Test that the refreshed token is actually sent in the Authorization header."""
    _, mock_requests = setup_mocks
    mock_requests.side_effect = [
        Mock(status_code=401, headers={"x-ms-public-api-error-code": "TokenExpired"}),
        Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})),
    ]
    mock_token_credential = Mock()
    mock_token_credential.get_token.side_effect = [
        Mock(token="stale_token", expires_on=9999999999),
        Mock(token="fresh_token", expires_on=9999999999),
    ]
    monkeypatch.setattr("fabric_cicd._common._fabric_endpoint._format_invoke_log", lambda *_, **__: "")

    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    endpoint.invoke("GET", "http://example.com")

    # Second request should use the fresh token
    second_call_headers = mock_requests.call_args_list[1][1]["headers"]
    assert second_call_headers["Authorization"] == "Bearer fresh_token"


def test_invoke_exception(setup_mocks):
    """Test that a generic exception during request is wrapped in InvokeError."""
    _, mock_requests = setup_mocks
    mock_requests.side_effect = Exception("Test exception")
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
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
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
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
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    # Mock time.sleep to avoid delays in tests
    monkeypatch.setattr("time.sleep", lambda _: None)

    response = endpoint.invoke("POST", "http://example.com", poll_long_running=True)

    # Should poll and return final status
    assert response["status_code"] == 200
    assert mock_requests.call_count == 2  # Initial request + polling request


def test_invoke_connection_error_retries_then_succeeds(setup_mocks, monkeypatch):
    """Test that connection errors are retried and succeed on subsequent attempt."""
    dl, mock_requests = setup_mocks
    mock_requests.side_effect = [
        requests.exceptions.ConnectionError("Connection refused"),
        Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})),
    ]
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    monkeypatch.setattr("time.sleep", lambda _: None)

    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke("GET", "http://example.com")

    assert response["status_code"] == 200
    assert mock_requests.call_count == 2
    assert any("Connection error encountered." in msg for msg in dl.messages)


def test_invoke_connection_error_exceeds_max_duration(setup_mocks, monkeypatch):
    """Test that persistent connection errors raise InvokeError after max_duration."""
    _, mock_requests = setup_mocks
    mock_requests.side_effect = requests.exceptions.ConnectionError("Connection refused")
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    monkeypatch.setattr("time.sleep", lambda _: None)

    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    with pytest.raises(InvokeError):
        endpoint.invoke("GET", "http://example.com", max_duration=0)


def test_invoke_timeout_retries_then_succeeds(setup_mocks, monkeypatch):
    """Test that Timeout errors are retried and succeed on subsequent attempt."""
    dl, mock_requests = setup_mocks
    mock_requests.side_effect = [
        requests.exceptions.Timeout("Request timed out"),
        Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})),
    ]
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    monkeypatch.setattr("time.sleep", lambda _: None)

    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    response = endpoint.invoke("GET", "http://example.com")

    assert response["status_code"] == 200
    assert mock_requests.call_count == 2
    assert any("Connection error encountered." in msg for msg in dl.messages)


def test_invoke_timeout_exceeds_max_duration(setup_mocks, monkeypatch):
    """Test that persistent Timeout errors raise InvokeError after max_duration."""
    _, mock_requests = setup_mocks
    mock_requests.side_effect = requests.exceptions.Timeout("Request timed out")
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    monkeypatch.setattr("time.sleep", lambda _: None)

    endpoint = FabricEndpoint(token_credential=mock_token_credential)

    with pytest.raises(InvokeError):
        endpoint.invoke("GET", "http://example.com", max_duration=0)


def test_invoke_calls_http_tracer(setup_mocks):
    """Test that invoke calls http_tracer capture methods and save."""
    _, mock_requests = setup_mocks
    mock_requests.return_value = Mock(
        status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={})
    )
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token=generate_mock_token(), expires_on=9999999999)
    mock_tracer = Mock()

    endpoint = FabricEndpoint(token_credential=mock_token_credential, http_tracer=mock_tracer)
    endpoint.invoke("GET", "http://example.com")

    mock_tracer.capture_request.assert_called_once()
    mock_tracer.capture_response.assert_called_once()
    mock_tracer.save.assert_called_once()


def test_get_token(setup_mocks):
    """Test getting token returns token from credential and caches it."""
    _dl, _mock_requests = setup_mocks
    mock_token_credential = Mock()
    mock_token_credential.get_token.return_value = Mock(token="test_token", expires_on=9999999999)
    endpoint = FabricEndpoint(token_credential=mock_token_credential)
    assert endpoint._get_token() == "test_token"
    assert endpoint._get_token() == "test_token"  # Second call uses cache
    # Only called once at init — subsequent calls return cached
    mock_token_credential.get_token.assert_called_once_with("https://api.fabric.microsoft.com/.default")


@pytest.mark.parametrize(
    ("raise_exception", "expected_msg"),
    [
        (ClientAuthenticationError("Auth failed"), "Failed to acquire Microsoft Entra token. Auth failed"),
        (
            Exception("Unexpected error"),
            "An unexpected error occurred when generating the Microsoft Entra token. Unexpected error",
        ),
    ],
    ids=["auth_error", "unexpected_exception"],
)
def test_get_token_exceptions(raise_exception, expected_msg):
    """Test token acquisition exception handling for authentication failures."""
    credential = DummyCredential("irrelevant")
    credential.raise_exception = raise_exception
    with pytest.raises(TokenError, match=expected_msg):
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
    response = Mock(status_code=status_code, headers=response_header, json=Mock(return_value=response_json), text="{}")

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
            5,
            False,
            {"Content-Type": "application/json"},
            {"message": "Internal Server Error"},
            r"Maximum execution duration \(0 seconds\) exceeded",
            0,
            0.0,
        ),
        (429, 5, True, {"Retry-After": "10"}, {}, r"Maximum execution duration \(0 seconds\) exceeded", 0, 0.0),
        (
            200,
            5,
            True,
            {},
            {"status": "Running"},
            r"Maximum execution duration \(0 seconds\) exceeded",
            0,
            0.0,
        ),
    ],
    ids=[
        "unauthorized",
        "principal_type_not_supported",
        "failed_library_removal",
        "retry_500",
        "retry_429",
        "long_running_timeout",
    ],
)
def test_handle_response_exceptions(
    status_code,
    input_iteration_count,
    input_long_running,
    response_header,
    return_value,
    exception_match,
    max_duration,
    start_time,
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


def test_handle_response_202_no_location_exits(setup_mocks):
    """Test 202 with no Location header exits immediately."""
    _, _ = setup_mocks
    response = Mock(status_code=202, headers={}, json=Mock(return_value={}), text="")
    exit_loop, _method, _url, _body, _long_running = _handle_response(
        response=response, method="POST", url="old", body="{}", long_running=False, iteration_count=1
    )
    assert exit_loop is True


def test_handle_retry_exponential_backoff(monkeypatch):
    """Test that handle_retry calculates exponential backoff correctly."""
    sleep_values = []
    monkeypatch.setattr("time.sleep", lambda x: sleep_values.append(x))

    handle_retry(attempt=1, base_delay=10, response_retry_after=60, max_duration=300, start_time=time.time())
    # delay = min(60, 10 * 2^1) = min(60, 20) = 20
    assert sleep_values[-1] == 20


def test_handle_retry_capped_by_retry_after(monkeypatch):
    """Test that delay is capped by Retry-After header value."""
    sleep_values = []
    monkeypatch.setattr("time.sleep", lambda x: sleep_values.append(x))

    handle_retry(attempt=10, base_delay=10, response_retry_after=30, max_duration=300, start_time=time.time())
    # delay = min(30, 10 * 2^10) = min(30, 10240) = 30
    assert sleep_values[-1] == 30


def test_handle_retry_override_env_var(monkeypatch):
    """Test that RETRY_DELAY_OVERRIDE_SECONDS env var overrides backoff calculation."""
    sleep_values = []
    monkeypatch.setattr("time.sleep", lambda x: sleep_values.append(x))
    monkeypatch.setenv(constants.EnvVar.RETRY_DELAY_OVERRIDE_SECONDS.value, "0.01")

    handle_retry(attempt=5, base_delay=10, response_retry_after=60, max_duration=300, start_time=time.time())
    assert sleep_values[-1] == 0.01


def test_handle_retry_exceeds_max_duration():
    """Test that handle_retry raises when max_duration is exceeded."""
    with pytest.raises(Exception, match="Maximum execution duration"):
        handle_retry(attempt=1, base_delay=10, max_duration=0, start_time=0.0)


def test_handle_retry_no_max_duration(monkeypatch):
    """Test that handle_retry works when max_duration is None (no timeout)."""
    sleep_values = []
    monkeypatch.setattr("time.sleep", lambda x: sleep_values.append(x))

    handle_retry(attempt=1, base_delay=10, response_retry_after=60, max_duration=None, start_time=None)
    assert sleep_values[-1] == 20


def test_format_invoke_log():
    """Test formatting of the invoke log message."""
    response = Mock(status_code=200, headers={"Content-Type": "application/json"}, json=Mock(return_value={}))
    log_message = _format_invoke_log(response, "GET", "http://example.com", "{}")
    assert "Method: GET" in log_message
    assert "URL: http://example.com" in log_message
    assert "Response Status: 200" in log_message
    assert "Request Body:" in log_message
