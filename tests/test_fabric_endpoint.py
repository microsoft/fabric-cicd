# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import Mock

import pytest

from fabric_cicd._common._fabric_endpoint import _handle_response


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("requests.request")


def test_200():
    """Initial call to an item api that returns a long running redirect"""

    response = Mock()
    response.status_code = 200
    response.headers = {"Content-Type": "application/json"}
    response.json.return_value = {"status": "Succeeded"}

    orig_method = "POST"
    orig_url = "http://example.com"
    orig_body = "{}"
    orig_long_running = False
    orig_retry_after = 60
    orig_iteration_count = 1

    exit_loop, method, url, body, long_running = _handle_response(
        response=response,
        method=orig_method,
        url=orig_url,
        body=orig_body,
        long_running=orig_long_running,
        retry_after=orig_retry_after,
        iteration_count=orig_iteration_count,
    )

    assert exit_loop is True
    assert method == orig_method
    assert url == orig_url
    assert body == orig_body
    assert long_running == orig_long_running


def test_202_long_running_redirect():
    """Initial call to an item api that returns a long running redirect"""
    request_method = "POST"
    request_url = "https://example.com/"
    request_body = '{"displayName": "Example"}'
    old_long_running = False
    old_iteration_count = 1

    response = Mock()
    response.status_code = 202
    response.headers = {
        "Content-Type": "application/json",
        "Location": "https://example.com/operations/0000",
        "x-ms-operation-id": "0acd697c-1550-43cd-b998-91bfbfbd47c6",
        "Retry-After": 30,
    }
    response.json.return_value = {}

    exit_loop, method, url, body, long_running = _handle_response(
        response=response,
        method=request_method,
        url=request_url,
        body=request_body,
        long_running=old_long_running,
        iteration_count=old_iteration_count,
    )

    assert exit_loop is False
    assert method == "GET"
    assert url == response.headers.get("Location")
    assert body == "{}"
    assert long_running != old_long_running


def test_200_long_running_inprogess():
    """The first get call to long running with in progess state"""
    request_method = "GET"
    request_url = "https://example.com/operations/0000"
    request_body = "{}"
    old_long_running = True
    old_iteration_count = 2

    response = Mock()
    response.status_code = 200
    response.headers = {
        "Content-Type": "application/json",
        "Location": "https://example.com/operations/0000",
        "x-ms-operation-id": "cfafbeb1-8037-4d0c-896e-a46fb27ff227",
        "Retry-After": 20,
    }
    response.json.return_value = {
        "status": "Running",
        "createdTimeUtc": "2023-09-13T14:56:18.477Z",
        "lastUpdatedTimeUtc": "2023-09-13T15:01:10.532Z",
        "percentComplete": 25,
    }

    exit_loop, method, url, body, long_running = _handle_response(
        response=response,
        method=request_method,
        url=request_url,
        body=request_body,
        long_running=old_long_running,
        iteration_count=old_iteration_count,
    )

    assert exit_loop is False
    assert method == "GET"
    assert url == response.headers.get("Location")
    assert body == "{}"
    assert long_running == old_long_running


def test_200_long_running_success():
    """The first get call to long running with in progess state"""
    request_method = "GET"
    request_url = "https://example.com/operations/0000"
    request_body = "{}"
    old_long_running = True
    old_iteration_count = 2

    response = Mock()
    response.status_code = 200
    response.headers = {
        "Content-Type": "application/json",
        "Location": "https://example.com/operations/0000/result",
        "x-ms-operation-id": "cfafbeb1-8037-4d0c-896e-a46fb27ff227",
        "Retry-After": 20,
    }
    response.json.return_value = {
        "status": "Succeeded",
        "createdTimeUtc": "2023-09-13T14:56:18.477Z",
        "lastUpdatedTimeUtc": "2023-09-13T15:01:10.532Z",
        "percentComplete": 100,
    }

    exit_loop, method, url, body, long_running = _handle_response(
        response=response,
        method=request_method,
        url=request_url,
        body=request_body,
        long_running=old_long_running,
        iteration_count=old_iteration_count,
    )

    assert exit_loop is False
    assert method == "GET"
    assert url == response.headers.get("Location")
    assert body == "{}"
    assert long_running == False


def test_200_long_running_success_withoutlocation():
    """The first get call to long running with in progess state"""
    request_method = "GET"
    request_url = "https://example.com/operations/0000"
    request_body = "{}"
    old_long_running = True
    old_iteration_count = 2

    response = Mock()
    response.status_code = 200
    response.headers = {
        "Content-Type": "application/json",
        "x-ms-operation-id": "cfafbeb1-8037-4d0c-896e-a46fb27ff227",
        "Retry-After": 20,
    }
    response.json.return_value = {
        "status": "Succeeded",
        "createdTimeUtc": "2023-09-13T14:56:18.477Z",
        "lastUpdatedTimeUtc": "2023-09-13T15:01:10.532Z",
        "percentComplete": 100,
    }

    exit_loop, method, url, body, long_running = _handle_response(
        response=response,
        method=request_method,
        url=request_url,
        body=request_body,
        long_running=old_long_running,
        iteration_count=old_iteration_count,
    )

    assert exit_loop is True
    assert long_running == False


def test_200_long_running_fail():
    """The first get call to long running with in progess state"""
    request_method = "GET"
    request_url = "https://example.com/operations/0000"
    request_body = "{}"
    old_long_running = True
    old_iteration_count = 2

    response = Mock()
    response.status_code = 200
    response.headers = {
        "Content-Type": "application/json",
        "x-ms-operation-id": "cfafbeb1-8037-4d0c-896e-a46fb27ff227",
        "Retry-After": 20,
    }
    response.json.return_value = {
        "status": "Failed",
        "error": {"errorCode": "SampleErrorCode", "message": "Sample failure message"},
    }

    with pytest.raises(Exception, match="[Operation failed].*"):
        _handle_response(
            response=response,
            method=request_method,
            url=request_url,
            body=request_body,
            long_running=old_long_running,
            iteration_count=old_iteration_count,
        )
