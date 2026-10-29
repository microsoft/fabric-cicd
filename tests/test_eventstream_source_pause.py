# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the enable_eventstream_source_pause feature flag on Eventstream definition updates."""

import logging
import traceback
from unittest.mock import MagicMock

import pytest

from fabric_cicd import FabricWorkspace, constants, get_supported_feature_flags
from fabric_cicd._common._exceptions import BaseCustomError, InvokeError, ItemCleanupError, PublishError
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher, ParallelConfig
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


@pytest.fixture
def orchestration_publisher():
    publisher, mock_workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )
    workspace = FabricWorkspace.__new__(FabricWorkspace)
    workspace._api_root_url = "https://api.example.com"
    workspace.workspace_id = EVENTSTREAM_GUID
    workspace.endpoint = mock_workspace.endpoint
    workspace.items_to_include = None
    workspace.item_type_in_scope = [ItemType.EVENTSTREAM.value]
    workspace.bulk_publish_enabled = False
    workspace.contains_param_item_vars = False
    workspace.publish_item_name_exclude_regex = None
    workspace.publish_folder_path_exclude_regex = None
    workspace.publish_folder_path_to_include = None
    item = _make_item()
    workspace.repository_items = {ItemType.EVENTSTREAM.value: {item.name: item}}
    publisher.fabric_workspace_obj = workspace
    return publisher, workspace


def test_flag_off_is_noop():
    """Without the flag, the context makes no API calls (publish behavior unchanged)."""
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    with publisher._publish_lifecycle([_make_item()]):
        pass

    workspace.endpoint.invoke.assert_not_called()


def test_cleanup_flag_off_is_noop():
    """Cleanup without the flag needs no setup state and makes no API calls."""
    publisher, workspace = _make_publisher()

    publisher.cleanup_items([_make_item()])

    workspace.endpoint.invoke.assert_not_called()


def test_flag_on_no_running_sources_updates_without_pause():
    """With the flag, an eventstream with no running sources is published without pause/resume calls."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Stopped"}],
    )

    with publisher._publish_lifecycle([_make_item()]):
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

    with publisher._publish_lifecycle([_make_item()]):
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

    with publisher._publish_lifecycle([_make_item()]):
        pass

    invoke_calls = workspace.endpoint.invoke.call_args_list
    urls = [call.kwargs["url"] for call in invoke_calls]
    # The topology was polled multiple times while the source settled from Pausing -> Paused.
    assert sum(1 for url in urls if url.endswith("/topology")) >= 3
    # Resume is still issued once the source has settled.
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


def test_flag_on_missing_source_remains_pending_until_it_reappears(monkeypatch):
    """A temporarily missing source remains pending instead of being treated as paused."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    monkeypatch.setenv("FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS", "0")

    workspace = MagicMock()
    workspace.base_api_url = f"https://api.example.com/v1/workspaces/{EVENTSTREAM_GUID}"
    topology_responses = [
        [{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
        [],
        [{"id": SOURCE_ID, "name": "MySource", "status": "Paused"}],
    ]

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            sources = topology_responses.pop(0) if topology_responses else []
            return {"body": {"sources": sources}}
        return {"body": {}}

    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke = MagicMock(side_effect=mock_invoke)
    publisher = EventstreamPublisher.__new__(EventstreamPublisher)
    publisher.fabric_workspace_obj = workspace

    with publisher._publish_lifecycle([_make_item()]):
        pass

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert sum(1 for url in urls if url.endswith("/topology")) == 3
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


def test_flag_on_pause_failed_stops_publish_and_resumes_source():
    """A terminal PauseFailed status aborts before publishing and runs registered cleanup."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    workspace = MagicMock()
    workspace.base_api_url = f"https://api.example.com/v1/workspaces/{EVENTSTREAM_GUID}"
    status = {"value": "Running"}

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            return {"body": {"sources": [{"id": SOURCE_ID, "name": "MySource", "status": status["value"]}]}}
        if method == "POST" and url.endswith(f"/sources/{SOURCE_ID}/pause"):
            status["value"] = "PauseFailed"
        elif method == "POST" and url.endswith(f"/sources/{SOURCE_ID}/resume"):
            status["value"] = "Running"
        return {"body": {}}

    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke = MagicMock(side_effect=mock_invoke)
    publisher = EventstreamPublisher.__new__(EventstreamPublisher)
    publisher.fabric_workspace_obj = workspace
    entered_publish = False

    with (
        pytest.raises(Exception, match="Failed to pause Eventstream source"),
        publisher._publish_lifecycle([_make_item()]),
    ):
        entered_publish = True

    assert not entered_publish
    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


@pytest.mark.parametrize("resume_fails", [False, True])
def test_setup_failure_resumes_sources_already_paused(resume_fails, caplog):
    """If setup fails midway, cleanup runs for sources paused before the failure."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    workspace = MagicMock()
    workspace.base_api_url = f"https://api.example.com/v1/workspaces/{EVENTSTREAM_GUID}"
    sources = [
        {"id": "source-1", "name": "SourceOne", "status": "Running"},
        {"id": "source-2", "name": "SourceTwo", "status": "Running"},
    ]

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            return {"body": {"sources": sources}}
        if method == "POST" and url.endswith("/sources/source-2/pause"):
            pause_error = Exception("Second pause failed")
            raise pause_error
        if resume_fails and url.endswith("/sources/source-1/resume"):
            resume_error = InvokeError("Resume failed", logging.getLogger(__name__))
            raise resume_error
        return {"body": {}}

    workspace.endpoint = MagicMock()
    workspace.endpoint.invoke = MagicMock(side_effect=mock_invoke)
    publisher = EventstreamPublisher.__new__(EventstreamPublisher)
    publisher.fabric_workspace_obj = workspace

    with pytest.raises(Exception, match="Second pause failed"), publisher._publish_lifecycle([_make_item()]):
        pass

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith("/sources/source-1/resume") for url in urls)
    assert not any(url.endswith("/sources/source-2/resume") for url in urls)
    if resume_fails:
        assert "Resume failed" in caplog.text


def test_flag_on_only_running_sources_are_paused():
    """Only running sources are paused; stopped sources are left untouched."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[
            {"id": "running-1", "name": "RunningSource", "status": "Running"},
            {"id": "stopped-1", "name": "StoppedSource", "status": "Stopped"},
        ],
    )

    with publisher._publish_lifecycle([_make_item()]):
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
    with pytest.raises(Exception, match="SomethingElse"), publisher._publish_lifecycle([_make_item()]):
        raise publish_error

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith(f"/sources/{SOURCE_ID}/pause") for url in urls)
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


@pytest.mark.parametrize("publish_fails", [False, True])
@pytest.mark.parametrize("source_count", [1, 2])
def test_resume_failures_are_reported_without_masking_publish_error(publish_fails, source_count, caplog):
    """All resumes are attempted; cleanup failures only propagate without a publish error."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[
            {"id": "source-1", "name": "SourceOne", "status": "Running"},
            {"id": "source-2", "name": "SourceTwo", "status": "Running"},
        ][:source_count],
    )
    invoke = workspace.endpoint.invoke.side_effect
    resume_errors = []

    def fail_resume(method, url, **kwargs):
        if url.endswith("/resume"):
            error = InvokeError(f"Resume failed for {url}", logging.getLogger(__name__))
            resume_errors.append(error)
            raise error
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = fail_resume
    publish_error = RuntimeError("Original publish failure")

    def publish():
        if publish_fails:
            raise publish_error

    expected_error = RuntimeError if publish_fails else ItemCleanupError
    with pytest.raises(expected_error) as raised, publisher._publish_lifecycle([_make_item()]):
        publish()

    assert len(resume_errors) == source_count
    resume_urls = [
        call.kwargs["url"]
        for call in workspace.endpoint.invoke.call_args_list
        if call.kwargs["url"].endswith("/resume")
    ]
    assert resume_urls == [
        f"{workspace.base_api_url}/eventstreams/{EVENTSTREAM_GUID}/sources/{source_id}/resume"
        for source_id in ("source-1", "source-2")[:source_count]
    ]
    if publish_fails:
        assert raised.value is publish_error
        error_text = caplog.text
    else:
        assert raised.value.errors == [
            (f"Resume Eventstream source 'source-{index}' in Eventstream '{EVENTSTREAM_GUID}'", error)
            for index, error in enumerate(resume_errors, start=1)
        ]
        assert raised.value.__cause__ is resume_errors[0]
        error_text = str(raised.value)
    for index in range(1, source_count + 1):
        assert f"source-{index}" in error_text
    assert EVENTSTREAM_GUID in error_text
    assert "Resume failed" in error_text


@pytest.mark.parametrize("publish_mode", ["standard", "bulk"])
@pytest.mark.parametrize("publish_fails", [False, True])
def test_multiple_resume_failures_are_reported_without_masking_original_error(
    publish_mode, publish_fails, orchestration_publisher, mocker, caplog
):
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = orchestration_publisher
    workspace.bulk_publish_enabled = publish_mode == "bulk"
    _, mock_workspace = _make_publisher(
        topology_sources=[
            {"id": "source-1", "name": "SourceOne", "status": "Running"},
            {"id": "source-2", "name": "SourceTwo", "status": "Running"},
        ]
    )
    workspace.endpoint = mock_workspace.endpoint
    invoke = workspace.endpoint.invoke.side_effect
    resume_errors = {
        source_id: InvokeError(
            f"Resume failed for {source_id}", logging.getLogger(__name__), f"Request log: {source_id}"
        )
        for source_id in ("source-1", "source-2")
    }

    def fail_resume(method, url, **kwargs):
        if url.endswith("/resume"):
            source_id = url.split("/sources/")[1].split("/")[0]
            raise resume_errors[source_id]
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = fail_resume
    publish_error = RuntimeError("Publish failed")
    post_error = RuntimeError("Post-publish failed")
    mocker.patch.object(publisher, "publish_one", side_effect=publish_error if publish_fails else None)
    mocker.patch.object(workspace, "_publish_items", side_effect=publish_error if publish_fails else None)
    mocker.patch.object(ItemPublisher, "create", return_value=publisher)
    post_publish = mocker.patch.object(publisher, "post_publish_all", side_effect=post_error)

    def run_publish():
        if publish_mode == "bulk":
            ItemPublisher.publish_all_bulk(workspace)
        else:
            publisher.publish_all()

    expected_error = ItemCleanupError
    if publish_fails:
        expected_error = PublishError if publish_mode == "standard" else RuntimeError
    with pytest.raises(expected_error) as raised:
        run_publish()

    if publish_fails:
        error = raised.value
        if publish_mode == "bulk":
            assert error is publish_error
        else:
            assert error.errors == [("MyEventstream", publish_error)]
            assert error.post_publish_error is post_error
            assert error.__cause__ is post_error
    else:
        assert raised.value.errors == [
            (f"Resume Eventstream source '{source_id}' in Eventstream '{EVENTSTREAM_GUID}'", error)
            for source_id, error in resume_errors.items()
        ]
        for source_id in resume_errors:
            assert f"Request log: {source_id}" in raised.value.additional_info

    error_text = caplog.text if publish_fails else str(raised.value)
    for source_id in resume_errors:
        assert f"Eventstream source '{source_id}' in Eventstream '{EVENTSTREAM_GUID}'" in error_text
    if publish_fails and publish_mode == "standard":
        post_publish.assert_called_once_with()
    else:
        post_publish.assert_not_called()


def test_eventstream_cleanup_collects_each_resume_failure(mocker):
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher()
    sources = [{"id": "source-1"}, {"id": "source-2"}, {"id": "source-3"}]
    publisher._paused_sources = [(EVENTSTREAM_GUID, source) for source in sources]
    errors = [RuntimeError("First resume failed"), RuntimeError("Last resume failed")]
    resume = mocker.patch("fabric_cicd._items._eventstream._resume_source", side_effect=[errors[0], None, errors[1]])

    with pytest.raises(ItemCleanupError) as raised:
        publisher.cleanup_items([_make_item()])

    assert resume.call_args_list == [mocker.call(workspace, EVENTSTREAM_GUID, source) for source in sources]
    assert isinstance(raised.value, BaseCustomError)
    assert raised.value.logger.name == "fabric_cicd._items._eventstream"
    assert raised.value.errors == [
        (f"Resume Eventstream source 'source-1' in Eventstream '{EVENTSTREAM_GUID}'", errors[0]),
        (f"Resume Eventstream source 'source-3' in Eventstream '{EVENTSTREAM_GUID}'", errors[1]),
    ]
    assert raised.value.__cause__ is errors[0]
    assert publisher._paused_sources == []
    assert "2 item cleanup action(s)" in str(raised.value)
    assert "source-1" in str(raised.value)
    assert "source-3" in str(raised.value)
    for error in errors:
        assert str(error) in raised.value.additional_info
    assert "Traceback" not in raised.value.additional_info


def test_standard_publish_failure_survives_resume_failure(mocker, caplog):
    """Collected standard-publish errors remain primary when cleanup also fails."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )
    item = _make_item()
    workspace.repository_items = {ItemType.EVENTSTREAM.value: {item.name: item}}
    workspace.items_to_include = None
    workspace._apply_publish_filters.return_value = False
    invoke = workspace.endpoint.invoke.side_effect

    def fail_resume(method, url, **kwargs):
        if url.endswith("/resume"):
            error = InvokeError("Resume failed", logging.getLogger(__name__))
            raise error
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = fail_resume
    publish_error = RuntimeError("Original publish failure")
    mocker.patch.object(publisher, "publish_one", side_effect=publish_error)
    post_publish = mocker.patch.object(publisher, "post_publish_all")

    with pytest.raises(PublishError) as raised:
        publisher.publish_all()

    assert raised.value.errors == [(item.name, publish_error)]
    post_publish.assert_called_once_with()
    assert "Resume failed" in caplog.text


def test_pause_timeout_survives_single_resume_conflict(mocker, caplog):
    """A failed resume is logged without retrying or replacing the pause timeout."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher()
    pause_timeout = TimeoutError("Original pause timeout")
    retry = mocker.patch(
        "fabric_cicd._items._eventstream.handle_retry",
        side_effect=pause_timeout,
    )
    paused = False
    resume_attempts = 0

    def invoke(method, url, **_kwargs):
        nonlocal paused, resume_attempts
        if method == "GET":
            status = "Pausing" if paused else "Running"
            return {"body": {"sources": [{"id": SOURCE_ID, "name": "MySource", "status": status}]}}
        if url.endswith("/pause"):
            paused = True
        elif url.endswith("/resume"):
            resume_attempts += 1
            error = InvokeError(
                "DataSourceStatusConflict: source is still transitioning",
                logging.getLogger(__name__),
            )
            raise error
        return {"body": {}}

    workspace.endpoint.invoke.side_effect = invoke
    with pytest.raises(TimeoutError) as raised, publisher._publish_lifecycle([_make_item()]):
        pytest.fail("Publishing must not start after pause timeout")

    assert raised.value is pause_timeout
    assert resume_attempts == 1
    retry.assert_called_once()
    assert "DataSourceStatusConflict" in caplog.text


@pytest.mark.parametrize("error_message", ["Access denied", "DataSourceStatusConflict"])
def test_resume_does_not_retry_api_failures(error_message, mocker):
    """Resume attempts once, including when the source is still transitioning."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )
    invoke = workspace.endpoint.invoke.side_effect
    error = InvokeError(error_message, logging.getLogger(__name__))
    retry = mocker.patch("fabric_cicd._items._eventstream.handle_retry")

    def fail_resume(method, url, **kwargs):
        if url.endswith("/resume"):
            raise error
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = fail_resume
    with pytest.raises(ItemCleanupError) as raised, publisher._publish_lifecycle([_make_item()]):
        pass

    assert raised.value.errors == [
        (f"Resume Eventstream source '{SOURCE_ID}' in Eventstream '{EVENTSTREAM_GUID}'", error)
    ]
    assert raised.value.__cause__ is error
    resume_calls = [call for call in workspace.endpoint.invoke.call_args_list if call.kwargs["url"].endswith("/resume")]
    assert len(resume_calls) == 1
    retry.assert_not_called()


def test_flag_on_new_eventstream_without_guid_skips_pause():
    """A brand-new eventstream (no GUID) is published directly without any topology/pause calls."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )

    with publisher._publish_lifecycle([_make_item(guid="")]):
        pass

    workspace.endpoint.invoke.assert_not_called()


@pytest.mark.parametrize("filter_type", ["name", "folder_exclude", "folder_include", "items_to_include"])
@pytest.mark.parametrize("include_publishable_item", [False, True])
@pytest.mark.parametrize("publish_mode", ["parallel", "sequential", "ordered", "bulk"])
def test_publish_filters_eventstreams_before_setup(
    filter_type, include_publishable_item, publish_mode, orchestration_publisher, mocker
):
    """Excluded Eventstreams never enter setup or publishing, even alongside included items."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = orchestration_publisher
    workspace.bulk_publish_enabled = publish_mode == "bulk"
    workspace.publish_item_name_exclude_regex = "^MyEventstream$" if filter_type == "name" else None
    workspace.publish_folder_path_exclude_regex = "^Excluded$" if filter_type == "folder_exclude" else None
    workspace.publish_folder_path_to_include = ["Included"] if filter_type == "folder_include" else None

    excluded_item = _make_item()
    excluded_item.folder_path = "Excluded"
    included_item = Item(
        type=ItemType.EVENTSTREAM.value,
        name="IncludedEventstream",
        description="",
        guid="966a66bd-74b3-4bc5-a34d-6878ca1a40d1",
        folder_path="Included",
    )
    if filter_type == "items_to_include":
        workspace.items_to_include = [f"{included_item.name}.{ItemType.EVENTSTREAM.value}"]
    items = {excluded_item.name: excluded_item}
    if include_publishable_item:
        items[included_item.name] = included_item
    workspace.repository_items = {ItemType.EVENTSTREAM.value: items}
    publisher.fabric_workspace_obj = workspace
    mocker.patch.object(
        EventstreamPublisher,
        "parallel_config",
        ParallelConfig(
            enabled=publish_mode == "parallel",
            ordered_items_func=(lambda _publisher: list(items)) if publish_mode == "ordered" else None,
        ),
    )
    mocker.patch.object(publisher, "pre_publish_all")
    post_publish = mocker.patch.object(publisher, "post_publish_all")
    publish_one = mocker.patch.object(publisher, "publish_one")
    publish_bulk = mocker.patch.object(workspace, "_publish_items")
    context = mocker.spy(publisher, "_publish_lifecycle")

    if publish_mode == "bulk":
        mocker.patch.object(ItemPublisher, "create", return_value=publisher)
        assert ItemPublisher.publish_all_bulk(workspace) == []
    else:
        publisher.publish_all()

    assert excluded_item.skip_publish is True
    assert workspace.repository_items[ItemType.EVENTSTREAM.value] == items
    if publish_mode != "bulk" or include_publishable_item:
        post_publish.assert_called_once_with()
    else:
        post_publish.assert_not_called()
    if include_publishable_item:
        assert included_item.skip_publish is False
        context.assert_called_once_with([included_item])
        if publish_mode == "bulk":
            publish_one.assert_not_called()
            publish_bulk.assert_called_once_with(
                [(included_item.name, included_item, publisher)],
                skipped_items=[f"{ItemType.EVENTSTREAM.value}: {excluded_item.name}"],
            )
        else:
            publish_one.assert_called_once_with(included_item.name, included_item)
            publish_bulk.assert_not_called()
        urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
        assert urls
        assert all(f"/eventstreams/{included_item.guid}/" in url for url in urls)
        assert any(url.endswith("/pause") for url in urls)
        assert any(url.endswith("/resume") for url in urls)
    else:
        context.assert_not_called()
        publish_one.assert_not_called()
        publish_bulk.assert_not_called()
        workspace.endpoint.invoke.assert_not_called()


@pytest.mark.parametrize("publish_mode", ["parallel", "sequential", "ordered", "bulk"])
@pytest.mark.parametrize("outcome", ["success", "publish_failure", "resume_failure", "publish_and_resume_failure"])
def test_publish_orchestration_orders_pause_publish_and_resume(
    publish_mode, outcome, orchestration_publisher, mocker, caplog
):
    """Both orchestrators wait for pause, publish, then clean up without masking failures."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = orchestration_publisher
    workspace.bulk_publish_enabled = publish_mode == "bulk"
    item = workspace.repository_items[ItemType.EVENTSTREAM.value]["MyEventstream"]
    mocker.patch.object(ItemPublisher, "create", return_value=publisher)
    mocker.patch.object(
        EventstreamPublisher,
        "parallel_config",
        ParallelConfig(
            enabled=publish_mode == "parallel",
            ordered_items_func=(lambda _publisher: [item.name]) if publish_mode == "ordered" else None,
        ),
    )
    events = []
    invoke = workspace.endpoint.invoke.side_effect
    publish_error = RuntimeError("Original publish failure")
    resume_error = InvokeError("Resume failed", logging.getLogger(__name__))
    publish_fails = outcome in {"publish_failure", "publish_and_resume_failure"}
    resume_fails = outcome in {"resume_failure", "publish_and_resume_failure"}

    def record_invoke(method, url, **kwargs):
        if url.endswith("/resume"):
            events.append("resume")
            if resume_fails:
                raise resume_error
        result = invoke(method, url, **kwargs)
        if url.endswith("/topology"):
            events.append(f"topology:{result['body']['sources'][0]['status']}")
        elif url.endswith("/pause"):
            events.append("pause")
        return result

    def publish(*_args, **_kwargs):
        events.append("publish")
        if publish_fails:
            raise publish_error

    workspace.endpoint.invoke.side_effect = record_invoke
    mocker.patch.object(publisher, "pre_publish_all", side_effect=lambda: events.append("pre"))
    mocker.patch.object(publisher, "post_publish_all", side_effect=lambda: events.append("post"))
    mocker.patch.object(workspace, "_publish_item", side_effect=publish)
    mocker.patch.object(workspace, "_publish_items", side_effect=publish)

    def run_publish():
        if publish_mode == "bulk":
            ItemPublisher.publish_all_bulk(workspace)
        else:
            publisher.publish_all()

    if outcome == "success":
        run_publish()
    else:
        expected_error = (
            PublishError
            if publish_fails and publish_mode != "bulk"
            else RuntimeError
            if publish_fails
            else ItemCleanupError
        )
        with pytest.raises(expected_error) as raised:
            run_publish()
        if publish_fails and publish_mode != "bulk":
            assert raised.value.errors == [(item.name, publish_error)]
        elif publish_fails:
            assert raised.value is publish_error
        else:
            assert raised.value.errors == [
                (f"Resume Eventstream source '{SOURCE_ID}' in Eventstream '{EVENTSTREAM_GUID}'", resume_error)
            ]

    expected_events = ["pre", "topology:Running", "pause", "topology:Paused", "publish", "resume"]
    if outcome == "success" or (publish_fails and publish_mode != "bulk"):
        expected_events.append("post")
    assert events == expected_events
    if resume_fails and publish_fails:
        assert "Resume failed" in caplog.text


@pytest.mark.parametrize("first_batch_fails", [False, True])
def test_bulk_orchestration_cleans_up_each_batch_before_refresh(first_batch_fails, orchestration_publisher, mocker):
    """Real batch planning and publisher contexts resume sources before the next stage."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = orchestration_publisher
    workspace.bulk_publish_enabled = True
    workspace.contains_param_item_vars = True
    workspace._dynamic_var_cache = {"$workspace.$id": EVENTSTREAM_GUID, "$items.Eventstream.stale.$id": "stale"}
    first = workspace.repository_items[ItemType.EVENTSTREAM.value]["MyEventstream"]
    second = Item(
        type=ItemType.EVENTSTREAM.value,
        name="SecondEventstream",
        description="",
        guid="966a66bd-74b3-4bc5-a34d-6878ca1a40d1",
    )
    workspace.repository_items[ItemType.EVENTSTREAM.value][second.name] = second
    mocker.patch.object(ItemPublisher, "create", return_value=publisher)
    mocker.patch(
        "fabric_cicd._items._bulk_publish_dependencies.build_dynamic_variable_dependency_graph",
        return_value=[(f"{second.type}.{second.name}", f"{first.type}.{first.name}")],
    )
    mocker.patch(
        "fabric_cicd._items._bulk_publish_dependencies.get_async_attributes_to_wait_for",
        return_value={},
    )
    events = []
    invoke = workspace.endpoint.invoke.side_effect
    publish_error = RuntimeError("First batch publish failed")

    def record_invoke(method, url, **kwargs):
        if url.endswith("/pause") or url.endswith("/resume"):
            guid = url.split("/eventstreams/")[1].split("/")[0]
            events.append((url.rsplit("/", 1)[1], guid))
        return invoke(method, url, **kwargs)

    def publish(batch_items, **_kwargs):
        assert len(batch_items) == 1
        item_name, item, batch_publisher = batch_items[0]
        assert item_name == item.name
        assert batch_publisher is publisher
        events.append(("publish", item.guid))
        if first_batch_fails:
            raise publish_error

    workspace.endpoint.invoke.side_effect = record_invoke
    publish_bulk = mocker.patch.object(workspace, "_publish_items", side_effect=publish)
    refresh = mocker.patch.object(workspace, "_refresh_deployed_items", side_effect=lambda: events.append("refresh"))
    post_publish = mocker.patch.object(publisher, "post_publish_all")

    if first_batch_fails:
        with pytest.raises(RuntimeError) as raised:
            ItemPublisher.publish_all_bulk(workspace)
        assert raised.value is publish_error
        refresh.assert_not_called()
        post_publish.assert_not_called()
        assert publish_bulk.call_count == 1
    else:
        assert ItemPublisher.publish_all_bulk(workspace) == []
        refresh.assert_called_once_with()
        post_publish.assert_called_once_with()
        assert publish_bulk.call_count == 2
        assert workspace._dynamic_var_cache == {"$workspace.$id": EVENTSTREAM_GUID}

    expected_events = [("pause", first.guid), ("publish", first.guid), ("resume", first.guid)]
    if not first_batch_fails:
        expected_events.extend(["refresh", ("pause", second.guid), ("publish", second.guid), ("resume", second.guid)])
    assert events == expected_events


@pytest.mark.parametrize("failure_stage", [None, "setup", "publish"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_publish_lifecycle_preserves_errors(failure_stage, cleanup_fails, caplog, mocker):
    """The Eventstream context always cleans up and preserves the primary exception."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    source = {"id": SOURCE_ID, "name": "MySource", "status": "Running"}
    publisher, _ = _make_publisher(topology_sources=[source])
    events = []
    operation_error = RuntimeError("Original operation failure")
    cleanup_error = RuntimeError("Cleanup failed")

    def setup(*_args):
        events.append("setup")
        if failure_stage == "setup":
            raise operation_error

    def cleanup(*_args):
        events.append("cleanup")
        if cleanup_fails:
            raise cleanup_error

    mocker.patch("fabric_cicd._items._eventstream._wait_for_sources_paused", side_effect=setup)
    mocker.patch("fabric_cicd._items._eventstream._resume_source", side_effect=cleanup)

    def run_publish():
        with publisher._publish_lifecycle([_make_item()]):
            events.append("publish")
            if failure_stage == "publish":
                raise operation_error

    if failure_stage is not None or cleanup_fails:
        with pytest.raises(RuntimeError if failure_stage is not None else ItemCleanupError) as raised:
            run_publish()
        if failure_stage is not None:
            assert raised.value is operation_error
        else:
            assert raised.value.errors == [
                (f"Resume Eventstream source '{SOURCE_ID}' in Eventstream '{EVENTSTREAM_GUID}'", cleanup_error)
            ]
    else:
        run_publish()

    assert events == (["setup", "cleanup"] if failure_stage == "setup" else ["setup", "publish", "cleanup"])
    if cleanup_fails and failure_stage is not None:
        assert "Cleanup failed" in caplog.text


def test_base_publish_lifecycle_is_noop():
    """The base hook is a no-op context manager so other item types are unaffected."""
    publisher = ItemPublisher.__new__(ItemPublisher)

    with publisher._publish_lifecycle([_make_item()]):
        pass


@pytest.mark.parametrize("chain_kind", ["explicit", "implicit", "suppressed", "none"])
def test_single_cleanup_failure_preserves_exception_chain(chain_kind, mocker):
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, _ = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )
    underlying_error = OSError("Underlying connection failed")
    cleanup_error = InvokeError("Resume failed", logging.getLogger(__name__), "Resume request details")
    expected_cause = underlying_error if chain_kind == "explicit" else None
    expected_context = underlying_error if chain_kind in {"implicit", "suppressed"} else None
    expected_suppression = chain_kind in {"explicit", "suppressed"}
    cleanup_error.__cause__ = expected_cause
    cleanup_error.__context__ = expected_context
    cleanup_error.__suppress_context__ = expected_suppression
    mocker.patch("fabric_cicd._items._eventstream._resume_source", side_effect=cleanup_error)

    def run_publish():
        with publisher._publish_lifecycle([_make_item()]):
            pass

    with pytest.raises(ItemCleanupError) as raised:
        run_publish()

    error = raised.value
    assert error.__cause__ is cleanup_error
    assert cleanup_error.__cause__ is expected_cause
    assert cleanup_error.__context__ is expected_context
    assert cleanup_error.__suppress_context__ is expected_suppression
    assert "Resume request details" in error.additional_info
    formatted_traceback = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert ("Underlying connection failed" in formatted_traceback) == (chain_kind in {"explicit", "implicit"})
    assert "Underlying connection failed" not in error.additional_info


@pytest.mark.parametrize("failure_stage", [None, "setup", "publish"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_base_publish_lifecycle_runs_hooks(failure_stage, cleanup_fails, error_type, caplog, mocker):
    """The base lifecycle calls item-specific hooks and preserves active errors."""
    publisher = ItemPublisher.__new__(ItemPublisher)
    items = [_make_item()]
    events = []
    operation_error = error_type("Original operation failure")
    cleanup_error = RuntimeError("Cleanup failed")

    def setup(received_items):
        assert received_items is items
        events.append("setup")
        if failure_stage == "setup":
            raise operation_error

    def cleanup(received_items):
        assert received_items is items
        events.append("cleanup")
        if cleanup_fails:
            raise cleanup_error

    setup_hook = mocker.patch.object(publisher, "setup_items", side_effect=setup)
    cleanup_hook = mocker.patch.object(publisher, "cleanup_items", side_effect=cleanup)

    def run_publish():
        with publisher._publish_lifecycle(items):
            events.append("publish")
            if failure_stage == "publish":
                raise operation_error

    if failure_stage is not None or cleanup_fails:
        expected_error = operation_error if failure_stage is not None else cleanup_error
        with pytest.raises(type(expected_error)) as raised:
            run_publish()
        assert raised.value is expected_error
    else:
        run_publish()

    setup_hook.assert_called_once_with(items)
    cleanup_hook.assert_called_once_with(items)
    assert events == (["setup", "cleanup"] if failure_stage == "setup" else ["setup", "publish", "cleanup"])
    if cleanup_fails and failure_stage is not None:
        assert "Failed to clean up after item publishing" in caplog.text


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_eventstream_lifecycle_clears_state_between_operations(cleanup_fails, mocker):
    """Reusing a publisher does not resume sources tracked by a previous operation."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    source = {"id": SOURCE_ID, "name": "MySource", "status": "Running"}
    publisher, workspace = _make_publisher(topology_sources=[source])
    resume = mocker.patch(
        "fabric_cicd._items._eventstream._resume_source",
        side_effect=RuntimeError("Resume failed") if cleanup_fails else None,
    )

    if cleanup_fails:
        with (
            pytest.raises(ItemCleanupError, match="Resume failed"),
            publisher._publish_lifecycle([_make_item()]),
        ):
            pass
    else:
        with publisher._publish_lifecycle([_make_item()]):
            pass

    assert publisher._paused_sources == []
    resume.assert_called_once_with(workspace, EVENTSTREAM_GUID, source)
    constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    workspace.endpoint.invoke.reset_mock()

    with publisher._publish_lifecycle([_make_item()]):
        pass

    resume.assert_called_once_with(workspace, EVENTSTREAM_GUID, source)
    workspace.endpoint.invoke.assert_not_called()


def test_flag_in_supported_feature_flags():
    """The new flag is exposed via get_supported_feature_flags()."""
    assert FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value in get_supported_feature_flags()
