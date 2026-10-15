# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the enable_eventstream_source_pause feature flag on Eventstream definition updates."""

from unittest.mock import MagicMock

import pytest

from fabric_cicd import constants, get_supported_feature_flags
from fabric_cicd._common._item import Item
from fabric_cicd._items._eventstream import EventstreamPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

PRIVATE_ENDPOINT_ERROR = (
    "Operation failed. Error Code: DataSourcesValidationError. "
    "Error Message: Validation errors found for some nodes: MySource. "
    "Data source is currently using a private endpoint. "
    "Pause the data source before attempting to update."
)

EVENTSTREAM_GUID = "8c500070-073f-4a88-b478-8fabe1941c52"
SOURCE_ID = "e2886002-d696-4c05-969c-51361365cc24"


@pytest.fixture(autouse=True)
def _clear_feature_flags():
    """Clear the eventstream source pause feature flag before and after each test."""
    constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    yield
    constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)


def _make_item(guid: str = EVENTSTREAM_GUID) -> Item:
    """Create an Eventstream Item with the given GUID."""
    return Item(type=ItemType.EVENTSTREAM.value, name="MyEventstream", description="", guid=guid)


def _make_publisher(publish_side_effect=None, topology_sources=None):
    """
    Create an EventstreamPublisher with a mocked FabricWorkspace.

    Args:
        publish_side_effect: side_effect assigned to the mocked ``_publish_item``.
        topology_sources: list of source dicts returned by the mocked topology GET.
    """
    workspace = MagicMock()
    workspace.base_api_url = f"https://api.example.com/v1/workspaces/{EVENTSTREAM_GUID}"
    workspace._publish_item = MagicMock(side_effect=publish_side_effect)

    sources = topology_sources if topology_sources is not None else []

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            return {"body": {"sources": sources}}
        return {"body": {}}

    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke = MagicMock(side_effect=mock_invoke)

    publisher = EventstreamPublisher.__new__(EventstreamPublisher)
    publisher.fabric_workspace_obj = workspace
    return publisher, workspace


def test_flag_off_calls_publish_item_only():
    """Without the flag, publish_one just delegates to _publish_item and makes no API calls."""
    publisher, workspace = _make_publisher()

    publisher.publish_one("MyEventstream", _make_item())

    workspace._publish_item.assert_called_once_with(item_name="MyEventstream", item_type=ItemType.EVENTSTREAM.value)
    workspace.endpoint.invoke.assert_not_called()


def test_flag_on_success_no_pause_resume():
    """With the flag, a successful update performs no topology/pause/resume calls."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher()

    publisher.publish_one("MyEventstream", _make_item())

    workspace._publish_item.assert_called_once()
    workspace.endpoint.invoke.assert_not_called()


def test_flag_on_private_endpoint_error_pauses_retries_resumes():
    """With the flag, the running source is paused, the update retried, and the source resumed."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    # First publish raises the private-endpoint error, second (retry) succeeds.
    publisher, workspace = _make_publisher(
        publish_side_effect=[Exception(PRIVATE_ENDPOINT_ERROR), None],
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    publisher.publish_one("MyEventstream", _make_item())

    assert workspace._publish_item.call_count == 2

    invoke_calls = workspace.endpoint.invoke.call_args_list
    urls = [call.kwargs["url"] for call in invoke_calls]
    methods = [call.kwargs["method"] for call in invoke_calls]

    assert any(url.endswith("/topology") for url in urls)
    assert any(url.endswith(f"/sources/{SOURCE_ID}/pause") for url in urls)
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)
    assert methods.count("POST") == 2  # pause + resume

    resume_call = next(call for call in invoke_calls if call.kwargs["url"].endswith("/resume"))
    assert resume_call.kwargs["body"] == {"startType": "WhenLastStopped"}


def test_flag_on_retry_still_fails_source_is_resumed():
    """When the retried update also fails, the paused source is still resumed (finally path)."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    retry_error = Exception("Operation failed. Error Code: SomethingElse.")
    publisher, workspace = _make_publisher(
        publish_side_effect=[Exception(PRIVATE_ENDPOINT_ERROR), retry_error],
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    with pytest.raises(Exception, match="SomethingElse"):
        publisher.publish_one("MyEventstream", _make_item())

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith(f"/sources/{SOURCE_ID}/pause") for url in urls)
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


def test_flag_on_only_running_sources_are_paused():
    """Only running sources are paused; stopped sources are left untouched."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    publisher, workspace = _make_publisher(
        publish_side_effect=[Exception(PRIVATE_ENDPOINT_ERROR), None],
        topology_sources=[
            {"id": "running-1", "name": "RunningSource", "status": "Running"},
            {"id": "stopped-1", "name": "StoppedSource", "status": "Stopped"},
        ],
    )

    publisher.publish_one("MyEventstream", _make_item())

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith("/sources/running-1/pause") for url in urls)
    assert not any(url.endswith("/sources/stopped-1/pause") for url in urls)


def test_flag_on_non_matching_error_reraises_without_pause():
    """A non private-endpoint error is re-raised and triggers no topology/pause calls."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    publisher, workspace = _make_publisher(publish_side_effect=Exception("Operation failed. Error Code: ItemNotFound."))

    with pytest.raises(Exception, match="ItemNotFound"):
        publisher.publish_one("MyEventstream", _make_item())

    workspace.endpoint.invoke.assert_not_called()
    assert workspace._publish_item.call_count == 1


def test_flag_on_new_eventstream_without_guid_reraises():
    """A brand-new eventstream (no GUID) re-raises the error without any topology/pause calls."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    publisher, workspace = _make_publisher(publish_side_effect=Exception(PRIVATE_ENDPOINT_ERROR))

    with pytest.raises(Exception, match="DataSourcesValidationError"):
        publisher.publish_one("MyEventstream", _make_item(guid=""))

    workspace.endpoint.invoke.assert_not_called()


def test_flag_in_supported_feature_flags():
    """The new flag is exposed via get_supported_feature_flags()."""
    assert FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value in get_supported_feature_flags()
