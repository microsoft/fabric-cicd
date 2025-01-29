# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import Mock

import pytest

from fabric_cicd._common._fabric_endpoint import _handle_response


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("requests.request")


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
        (True, False,    {"status": "Running"}, {"Retry-After": 20, "Location": "old"}),
        (False, True,  {"status": "Succeeded"}, {}),
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
