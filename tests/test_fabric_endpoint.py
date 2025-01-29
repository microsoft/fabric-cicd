# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import Mock

import pytest

from fabric_cicd._common._fabric_endpoint import _handle_response


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch("requests.request")


@pytest.mark.parametrize( 
    ("status_code", "raise_exception", "request_method", "long_running", "expected_long_running", "expected_exit_loop", "response_json"),  
    [
        (200, False, "POST", False, False,  True, {}),
        (202, False, "POST", False,  True, False, {}),
        (200, False,  "GET",  True,  True, False, {"status": "Running"}),
        (200, False,  "GET",  True, False,  True, {"status": "Succeeded"}),
        (200, False,  "GET",  True, False, False, {"status": "Succeeded"}),
        (200,  True,  "GET",  True, False,  True, {"status": "Failed","error": {"errorCode": "SampleErrorCode", "message": "Sample failure message"}}),
    ],
    ids=[
        "success",
        "long_running_redirect",
        "long_running_running",
        "long_running__success",
        "long_running__success_with_result",
        "long_running__failed",
    ])  # fmt: skip
def test_handle_response(
    status_code,
    raise_exception,
    request_method,
    long_running,
    expected_long_running,
    expected_exit_loop,
    response_json,
):
    """Long running scenarios expected to pass"""
    response = Mock(status_code=status_code, headers={}, json=Mock(return_value=response_json))

    request_url = "old"

    if long_running and not expected_exit_loop:
        response.headers["Retry-After"] = 20
        response.headers["Location"] = request_url
    elif not long_running and expected_long_running:
        response.headers["Retry-After"] = 20
        response.headers["Location"] = "new"

    if not raise_exception:
        exit_loop, _method, url, _body, long_running = _handle_response(
            response=response,
            method=request_method,
            url=request_url,
            body="{}",
            long_running=long_running,
            iteration_count=2,
        )
        assert exit_loop == expected_exit_loop
        assert long_running == expected_long_running
    else:
        exception_match = "[Operation failed].*" if status_code == 200 else ""
        with pytest.raises(Exception, match=exception_match):
            _handle_response(
                response=response,
                method=request_method,
                url=request_url,
                body="{}",
                long_running=long_running,
                iteration_count=2,
            )
