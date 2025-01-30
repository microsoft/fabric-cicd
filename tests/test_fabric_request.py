# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from unittest.mock import Mock

import pytest
from requests import Response

from fabric_cicd._common._exceptions import LongRunningOperationError
from fabric_cicd._common._fabric_request import FabricRequest


@pytest.fixture
def fabric_request():
    return FabricRequest(endpoint=Mock(aad_token="fake_token"), method="POST", url="http://example.com")


def mock_response(status_code=200, headers=None, json_data=None):
    response = Mock(spec=Response)
    response.status_code = status_code
    response.headers = headers or {}
    response.json = Mock(return_value=json_data or {})
    return response


def test_process_long_running_success(fabric_request):
    fabric_request.response = mock_response(
        status_code=200, headers={"Location": "http://example.com/next"}, json_data={"status": "Running"}
    )
    fabric_request.retry_submit = Mock()

    fabric_request.process_long_running()

    assert fabric_request.method == "GET"
    assert fabric_request.url == "http://example.com/next"
    assert fabric_request.body == "{}"
    fabric_request.retry_submit.assert_called_once()


def test_process_long_running_failed(fabric_request):
    fabric_request.response = mock_response(
        status_code=200,
        headers={},
        json_data={
            "status": "Failed",
            "error": {"errorCode": "SampleErrorCode", "message": "Sample failure message"},
        },
    )

    with pytest.raises(
        LongRunningOperationError,
        match="Operation failed. Error Code: SampleErrorCode. Error Message: Sample failure message",
    ):
        fabric_request.process_long_running()


def test_process_long_running_undefined(fabric_request):
    fabric_request.response = mock_response(status_code=200, headers={}, json_data={"status": "Undefined"})

    with pytest.raises(
        LongRunningOperationError, match="Operation is in an undefined state. Full Body: {'status': 'Undefined'}"
    ):
        fabric_request.process_long_running()


def test_process_long_running_no_location(fabric_request):
    fabric_request.response = mock_response(status_code=200, headers={}, json_data={"status": "Running"})
    fabric_request.retry_submit = Mock()

    fabric_request.process_long_running()

    assert fabric_request.method == "GET"
    assert fabric_request.url is None
    assert fabric_request.body == "{}"
    fabric_request.retry_submit.assert_called_once()


def test_process_long_running_succeeded(fabric_request):
    fabric_request.response = mock_response(status_code=200, headers={}, json_data={"status": "Succeeded"})
    fabric_request.retry_submit = Mock()

    fabric_request.process_long_running()

    fabric_request.retry_submit.assert_not_called()

    with pytest.raises(
        LongRunningOperationError,
        match="Operation failed. Error Code: SampleErrorCode. Error Message: Sample failure message",
    ):
        fabric_request.process_long_running()


def test_process_long_running_undefined(fabric_request):
    fabric_request.response = Mock(status_code=200, headers={}, json=Mock(return_value={"status": "Undefined"}))

    with pytest.raises(
        LongRunningOperationError, match="Operation is in an undefined state. Full Body: {'status': 'Undefined'}"
    ):
        fabric_request.process_long_running()
