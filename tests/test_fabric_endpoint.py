# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
import json
from unittest.mock import Mock

import pytest

from fabric_cicd._common._fabric_endpoint import FabricEndpoint, _handle_response


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("requests.request")


def generate_mock_jwt():
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode().strip("=")
    payload = base64.urlsafe_b64encode(json.dumps({"exp": 9999999999}).encode()).decode().strip("=")
    signature = "signature"
    return f"{header}.{payload}.{signature}"


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
    import time

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
