# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the enable_eventstream_source_pause feature flag on Eventstream definition updates."""

from unittest.mock import MagicMock

import pytest

from fabric_cicd import constants, get_supported_feature_flags
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd._items._eventstream import EventstreamPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

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


def _make_publisher(topology_sources=None):
    """
    Create an EventstreamPublisher with a mocked FabricWorkspace.

    The mock is stateful: a successful POST to a source's /pause endpoint flips that
    source's topology status to 'Paused' (mirroring the asynchronous pause settling),
    and /resume flips it back to 'Running'.

    Args:
        topology_sources: list of source dicts returned by the mocked topology GET.
    """
    workspace = MagicMock()
    workspace.base_api_url = f"https://api.example.com/v1/workspaces/{EVENTSTREAM_GUID}"

    state = {source["id"]: dict(source) for source in (topology_sources or [])}

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            return {"body": {"sources": [dict(source) for source in state.values()]}}
        if method == "POST" and "/sources/" in url:
            source_id = url.split("/sources/")[1].split("/")[0]
            if url.endswith("/pause") and source_id in state:
                state[source_id]["status"] = "Paused"
            elif url.endswith("/resume") and source_id in state:
                state[source_id]["status"] = "Running"
        return {"body": {}}

    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke = MagicMock(side_effect=mock_invoke)

    publisher = EventstreamPublisher.__new__(EventstreamPublisher)
    publisher.fabric_workspace_obj = workspace
    return publisher, workspace


def test_flag_off_is_noop():
    """Without the flag, the context makes no API calls (publish behavior unchanged)."""
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    with publisher.publish_items_context([_make_item()]):
        pass

    workspace.endpoint.invoke.assert_not_called()


def test_flag_on_no_running_sources_updates_without_pause():
    """With the flag, an eventstream with no running sources is published without pause/resume calls."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Stopped"}],
    )

    with publisher.publish_items_context([_make_item()]):
        pass

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith("/topology") for url in urls)
    assert not any("/pause" in url for url in urls)
    assert not any("/resume" in url for url in urls)


def test_flag_on_pauses_running_sources_then_resumes():
    """With the flag, running sources are paused on enter and resumed on exit."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    with publisher.publish_items_context([_make_item()]):
        # Inside the context the source is paused but not yet resumed.
        urls_inside = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
        assert any(url.endswith(f"/sources/{SOURCE_ID}/pause") for url in urls_inside)
        assert not any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls_inside)

    invoke_calls = workspace.endpoint.invoke.call_args_list
    urls = [call.kwargs["url"] for call in invoke_calls]
    methods = [call.kwargs["method"] for call in invoke_calls]

    assert any(url.endswith("/topology") for url in urls)
    assert any(url.endswith(f"/sources/{SOURCE_ID}/pause") for url in urls)
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)
    assert methods.count("POST") == 2  # pause + resume

    resume_call = next(call for call in invoke_calls if call.kwargs["url"].endswith("/resume"))
    assert resume_call.kwargs["body"] == {"startType": "WhenLastStopped"}


def test_flag_on_waits_for_async_pause_before_resuming(monkeypatch):
    """Pause is asynchronous: resume must wait until the source leaves the transitional 'Pausing' state.

    Reproduces the real-world DataSourceStatusConflict where resume was called while the
    source was still 'Pausing'. The mock reports 'Pausing' for the first two topology polls
    after pause, then 'Paused'.
    """
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    # Avoid real backoff sleeps during the poll loop.
    monkeypatch.setenv("FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS", "0")

    workspace = MagicMock()
    workspace.base_api_url = f"https://api.example.com/v1/workspaces/{EVENTSTREAM_GUID}"

    status = {"value": "Running"}
    # Topology statuses returned in order once the source has been paused.
    pending_statuses = ["Pausing", "Pausing", "Paused"]

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            current = status["value"]
            if current == "Paused-pending":
                current = pending_statuses.pop(0) if pending_statuses else "Paused"
            return {"body": {"sources": [{"id": SOURCE_ID, "name": "MySource", "status": current}]}}
        if method == "POST" and url.endswith(f"/sources/{SOURCE_ID}/pause"):
            status["value"] = "Paused-pending"
        elif method == "POST" and url.endswith(f"/sources/{SOURCE_ID}/resume"):
            status["value"] = "Running"
        return {"body": {}}

    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke = MagicMock(side_effect=mock_invoke)
    publisher = EventstreamPublisher.__new__(EventstreamPublisher)
    publisher.fabric_workspace_obj = workspace

    with publisher.publish_items_context([_make_item()]):
        pass

    invoke_calls = workspace.endpoint.invoke.call_args_list
    urls = [call.kwargs["url"] for call in invoke_calls]
    # The topology was polled multiple times while the source settled from Pausing -> Paused.
    assert sum(1 for url in urls if url.endswith("/topology")) >= 3
    # Resume is still issued once the source has settled.
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


def test_flag_on_only_running_sources_are_paused():
    """Only running sources are paused; stopped sources are left untouched."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[
            {"id": "running-1", "name": "RunningSource", "status": "Running"},
            {"id": "stopped-1", "name": "StoppedSource", "status": "Stopped"},
        ],
    )

    with publisher.publish_items_context([_make_item()]):
        pass

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith("/sources/running-1/pause") for url in urls)
    assert not any(url.endswith("/sources/stopped-1/pause") for url in urls)
    assert any(url.endswith("/sources/running-1/resume") for url in urls)


def test_flag_on_publish_fails_sources_are_still_resumed():
    """When the publish fails inside the context, paused sources are still resumed (finally path)."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    publish_error = Exception("Operation failed. Error Code: SomethingElse.")
    with pytest.raises(Exception, match="SomethingElse"), publisher.publish_items_context([_make_item()]):
        raise publish_error

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith(f"/sources/{SOURCE_ID}/pause") for url in urls)
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


def test_flag_on_new_eventstream_without_guid_skips_pause():
    """A brand-new eventstream (no GUID) is published directly without any topology/pause calls."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    with publisher.publish_items_context([_make_item(guid="")]):
        pass

    workspace.endpoint.invoke.assert_not_called()


def test_base_publish_items_context_is_noop():
    """The base hook is a no-op context manager so other item types are unaffected."""
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    # Invoke the base implementation directly (bypassing the Eventstream override).
    with ItemPublisher.publish_items_context(publisher, [_make_item()]):
        pass

    workspace.endpoint.invoke.assert_not_called()


def test_flag_in_supported_feature_flags():
    """The new flag is exposed via get_supported_feature_flags()."""
    assert FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value in get_supported_feature_flags()
