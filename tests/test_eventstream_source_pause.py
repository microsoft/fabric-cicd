# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the enable_eventstream_source_pause feature flag on Eventstream definition updates."""

import base64
import json
import logging
import traceback
from unittest.mock import MagicMock

import pytest

from fabric_cicd import FabricWorkspace, constants, get_supported_feature_flags
from fabric_cicd._common._exceptions import BaseCustomError, InvokeError, ItemCleanupError, PublishError
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher, ParallelConfig
from fabric_cicd._items._environment import EnvironmentPublisher
from fabric_cicd._items._eventstream import EventstreamPublisher
from fabric_cicd._items._variablelibrary import VariableLibraryPublisher
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


def _make_publisher(topology_sources=None, *, invoke_side_effect=None):
    """
    Create an EventstreamPublisher with a mocked FabricWorkspace.

    The mock is stateful: a successful POST to a source's /pause endpoint flips that
    source's topology status to 'Paused' (mirroring the asynchronous pause settling),
    and /resume flips it back to 'Running'.

    Args:
        topology_sources: list of source dicts returned by the mocked topology GET.
        invoke_side_effect: optional endpoint behavior for custom API scenarios.
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
    workspace.endpoint.invoke = MagicMock(side_effect=mock_invoke if invoke_side_effect is None else invoke_side_effect)

    publisher = EventstreamPublisher.__new__(EventstreamPublisher)
    publisher.fabric_workspace_obj = workspace
    return publisher, workspace


def _fail_resumes(workspace, errors):
    """Inject per-source resume errors while preserving other endpoint behavior."""
    invoke = workspace.endpoint.invoke.side_effect

    def fail_resume(method, url, **kwargs):
        if url.endswith("/resume"):
            source_id = url.split("/sources/")[1].split("/")[0]
            if source_id in errors:
                raise errors[source_id]
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = fail_resume


def _definition_response(source_ids):
    """Build the deployed Eventstream definition response for the given sources."""
    payload = base64.b64encode(
        json.dumps({"sources": [{"id": source_id} for source_id in source_ids]}).encode()
    ).decode()
    return {
        "body": {
            "definition": {"parts": [{"path": "eventstream.json", "payload": payload, "payloadType": "InlineBase64"}]}
        }
    }


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


@pytest.mark.parametrize("settled_status", ["Running", "running", "Paused", "Stopped"])
def test_setup_waits_for_updating_sources_before_selecting_running_sources(settled_status, monkeypatch):
    """Wait for all updates, then pause and resume only sources that have become running."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    monkeypatch.setenv("FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS", "0")
    sources = [
        {"id": SOURCE_ID, "status": "Updating"},
        {"id": "source-2", "status": "updating"},
        {"id": "already-paused", "status": "Paused"},
    ]
    events = []
    topology_reads = 0

    def invoke(method, url, **_kwargs):
        nonlocal topology_reads
        if method == "GET":
            topology_reads += 1
            if topology_reads == 2:
                sources[0]["status"] = settled_status
            elif topology_reads == 3:
                sources[1]["status"] = "Running"
            events.append("topology")
            return {"body": {"sources": [dict(source) for source in sources]}}
        source_id = url.split("/sources/")[1].split("/")[0]
        action = url.rsplit("/", 1)[1]
        events.append(f"{action}:{source_id}")
        source = next(source for source in sources if source["id"] == source_id)
        source["status"] = "Paused" if action == "pause" else "Running"
        return {"body": {}}

    publisher, _ = _make_publisher(invoke_side_effect=invoke)
    with publisher._publish_lifecycle([_make_item()]):
        events.append("publish")
        expected_paused_ids = {SOURCE_ID, "source-2"} if settled_status.lower() == "running" else {"source-2"}
        assert {source["id"] for _, source in publisher._paused_sources} == expected_paused_ids
        assert all(source["status"] != "Updating" for source in sources)

    assert events[:3] == ["topology"] * 3
    expected_ids = [SOURCE_ID, "source-2"] if settled_status.lower() == "running" else ["source-2"]
    assert [event for event in events if event.startswith("pause:")] == [
        f"pause:{source_id}" for source_id in expected_ids
    ]
    assert [event for event in events if event.startswith("resume:")] == [
        f"resume:{source_id}" for source_id in expected_ids
    ]
    assert sources[2]["status"] == "Paused"


def test_setup_persistent_updating_times_out_without_publishing(monkeypatch):
    """An update that never settles aborts setup without pausing or resuming any source."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    monkeypatch.setattr(constants, "RETRY_MAX_DURATION_SECONDS", 0)
    publisher, workspace = _make_publisher(
        topology_sources=[
            {"id": SOURCE_ID, "status": "Updating"},
            {"id": "running-source", "status": "Running"},
        ],
    )

    with (
        pytest.raises(Exception, match=r"Maximum execution duration \(0 seconds\) exceeded"),
        publisher._publish_lifecycle([_make_item()]),
    ):
        pytest.fail("Publishing must not start while a source is Updating")

    assert publisher._paused_sources == []
    workspace.endpoint.invoke.assert_called_once_with(
        method="GET",
        url=f"{workspace.base_api_url}/eventstreams/{EVENTSTREAM_GUID}/topology",
    )


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


@pytest.mark.parametrize(
    ("source_metadata", "expected_start_type"),
    [
        ({"type": "SampleData"}, "Now"),
        ({"type": "AzureEventHub"}, "WhenLastStopped"),
        ({}, "WhenLastStopped"),
    ],
)
def test_flag_on_pauses_running_sources_then_resumes(source_metadata, expected_start_type):
    """Running sources pause on enter and resume with the appropriate start option."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running", **source_metadata}],
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
    assert resume_call.kwargs["body"] == {"startType": expected_start_type}


def test_flag_on_waits_for_async_pause_before_resuming(monkeypatch):
    """Pause is asynchronous: resume must wait until the source leaves the transitional 'Pausing' state.

    Reproduces the real-world DataSourceStatusConflict where resume was called while the
    source was still 'Pausing'. The mock reports 'Pausing' for the first two topology polls
    after pause, then 'Paused'.
    """
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    # Avoid real backoff sleeps during the poll loop.
    monkeypatch.setenv("FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS", "0")

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

    publisher, workspace = _make_publisher(invoke_side_effect=mock_invoke)

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

    topology_responses = [
        [{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
        [],
        [{"id": SOURCE_ID, "name": "MySource", "status": "Paused"}],
        [{"id": SOURCE_ID, "name": "MySource", "status": "Paused"}],
    ]

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            sources = topology_responses.pop(0) if topology_responses else []
            return {"body": {"sources": sources}}
        return {"body": {}}

    publisher, workspace = _make_publisher(invoke_side_effect=mock_invoke)

    with publisher._publish_lifecycle([_make_item()]):
        pass

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert sum(1 for url in urls if url.endswith("/topology")) == 4
    assert any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)


def test_flag_on_pause_failed_stops_publish_and_reports_recovery_failure(caplog):
    """A terminal PauseFailed status aborts publishing and is reported during cleanup."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

    status = {"value": "Running"}

    def mock_invoke(method, url, _body=None, **_kwargs):
        if method == "GET" and url.endswith("/topology"):
            return {"body": {"sources": [{"id": SOURCE_ID, "name": "MySource", "status": status["value"]}]}}
        if method == "POST" and url.endswith(f"/sources/{SOURCE_ID}/pause"):
            status["value"] = "PauseFailed"
        elif method == "POST" and url.endswith(f"/sources/{SOURCE_ID}/resume"):
            status["value"] = "Running"
        return {"body": {}}

    publisher, workspace = _make_publisher(invoke_side_effect=mock_invoke)
    entered_publish = False

    with (
        pytest.raises(Exception, match="Failed to pause Eventstream source"),
        publisher._publish_lifecycle([_make_item()]),
    ):
        entered_publish = True

    assert not entered_publish
    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert not any(url.endswith(f"/sources/{SOURCE_ID}/resume") for url in urls)
    assert "Cannot recover Eventstream source" in caplog.text
    assert "pausefailed" in caplog.text


@pytest.mark.parametrize("resume_fails", [False, True])
def test_setup_failure_resumes_sources_already_paused(resume_fails, caplog):
    """If setup fails midway, cleanup runs for sources paused before the failure."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)

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
        if method == "POST" and url.endswith("/sources/source-1/pause"):
            sources[0]["status"] = "Paused"
        if resume_fails and url.endswith("/sources/source-1/resume"):
            resume_error = InvokeError("Resume failed", logging.getLogger(__name__))
            raise resume_error
        return {"body": {}}

    publisher, workspace = _make_publisher(invoke_side_effect=mock_invoke)

    with pytest.raises(Exception, match="Second pause failed"), publisher._publish_lifecycle([_make_item()]):
        pass

    urls = [call.kwargs["url"] for call in workspace.endpoint.invoke.call_args_list]
    assert any(url.endswith("/sources/source-1/resume") for url in urls)
    assert not any(url.endswith("/sources/source-2/resume") for url in urls)
    if resume_fails:
        assert "Resume failed" in caplog.text


@pytest.mark.parametrize("accepted_status", ["Running", "Paused", "Pausing"])
def test_lost_pause_response_reconciles_source(accepted_status, monkeypatch):
    """Recover accepted pauses even when invoke raises, without resuming running sources."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    monkeypatch.setenv("FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS", "0")
    source = {"id": SOURCE_ID, "name": "MySource", "status": "Running"}
    pause_error = InvokeError("Pause response lost", logging.getLogger(__name__))
    statuses_at_resume = []
    topology_statuses = []

    def invoke(method, url, **_kwargs):
        if method == "GET":
            topology_statuses.append(source["status"])
            response = {"body": {"sources": [dict(source)]}}
            if source["status"] == "Pausing" and topology_statuses.count("Pausing") == 2:
                source["status"] = "Paused"
            return response
        if url.endswith("/pause"):
            source["status"] = accepted_status
            raise pause_error
        if url.endswith("/resume"):
            statuses_at_resume.append(source["status"])
            source["status"] = "Running"
        return {"body": {}}

    publisher, _ = _make_publisher(invoke_side_effect=invoke)
    with pytest.raises(InvokeError) as raised, publisher._publish_lifecycle([_make_item()]):
        pytest.fail("Publishing must not start after a pause request failure")

    assert raised.value is pause_error
    assert statuses_at_resume == ([] if accepted_status == "Running" else ["Paused"])
    assert source["status"] == "Running"
    assert publisher._paused_sources == []
    if accepted_status == "Pausing":
        assert topology_statuses == ["Running", "Pausing", "Pausing", "Paused"]


@pytest.mark.parametrize("status", [None, "Stopped", "PauseFailed", "Unknown"])
def test_cleanup_reports_unrecoverable_topology_and_continues(status):
    """Missing or unexpected topology states do not prevent recovery of other sources."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    sources = [{"id": "source-2", "status": "Paused"}]
    if status is not None:
        sources.append({"id": "source-1", "status": status})
    publisher, workspace = _make_publisher(topology_sources=sources)
    publisher._paused_sources = [(EVENTSTREAM_GUID, {"id": source_id}) for source_id in ("source-1", "source-2")]
    invoke = workspace.endpoint.invoke.side_effect

    def deployed_definition(method, url, **kwargs):
        if url.endswith("/getDefinition"):
            return _definition_response(["source-1", "source-2"])
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = deployed_definition

    with pytest.raises(ItemCleanupError) as raised:
        publisher.cleanup_items([_make_item()])

    assert len(raised.value.errors) == 1
    assert "source-1" in raised.value.errors[0][0]
    assert (status.lower() if status else "missing") in str(raised.value.errors[0][1])
    resume_urls = [
        call.kwargs["url"]
        for call in workspace.endpoint.invoke.call_args_list
        if call.kwargs["url"].endswith("/resume")
    ]
    assert resume_urls == [f"{workspace.base_api_url}/eventstreams/{EVENTSTREAM_GUID}/sources/source-2/resume"]
    assert publisher._paused_sources == []


@pytest.mark.parametrize("replacement_ids", [[], ["replacement-source"]])
def test_cleanup_skips_sources_removed_from_deployed_definition(replacement_ids, caplog):
    """A successful removal or replacement does not fail cleanup or resume a new source."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[
            {"id": "removed-source", "status": "Running"},
            {"id": "remaining-source", "status": "Running"},
        ],
    )
    invoke = workspace.endpoint.invoke.side_effect
    published = False

    def deployed_topology(method, url, **kwargs):
        if url.endswith("/getDefinition"):
            assert published
            return _definition_response(["remaining-source", *replacement_ids])
        result = invoke(method, url, **kwargs)
        if published and method == "GET":
            result["body"]["sources"] = [
                source for source in result["body"]["sources"] if source["id"] != "removed-source"
            ] + [{"id": source_id, "status": "Paused"} for source_id in replacement_ids]
        return result

    workspace.endpoint.invoke.side_effect = deployed_topology
    with publisher._publish_lifecycle([_make_item()]):
        published = True

    resume_urls = [
        call.kwargs["url"]
        for call in workspace.endpoint.invoke.call_args_list
        if call.kwargs["url"].endswith("/resume")
    ]
    assert resume_urls == [f"{workspace.base_api_url}/eventstreams/{EVENTSTREAM_GUID}/sources/remaining-source/resume"]
    definition_calls = [
        call for call in workspace.endpoint.invoke.call_args_list if call.kwargs["url"].endswith("/getDefinition")
    ]
    assert len(definition_calls) == 1
    assert definition_calls[0].kwargs == {
        "method": "POST",
        "url": f"{workspace.base_api_url}/items/{EVENTSTREAM_GUID}/getDefinition",
    }
    assert publisher._paused_sources == []
    assert "removed-source" in caplog.text
    assert "no longer in the deployed definition" in caplog.text


@pytest.mark.parametrize("definition_failure", ["lookup", "missing-part", "invalid-payload", "missing-sources"])
def test_cleanup_definition_failure_is_reported_and_other_sources_resume(definition_failure):
    """Unconfirmed removals remain errors and do not block recovery of other sources."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(topology_sources=[{"id": "remaining-source", "status": "Paused"}])
    publisher._paused_sources = [
        (EVENTSTREAM_GUID, {"id": source_id}) for source_id in ("missing-source", "remaining-source")
    ]
    invoke = workspace.endpoint.invoke.side_effect
    lookup_error = InvokeError("Definition unavailable", logging.getLogger(__name__))

    def failed_definition(method, url, **kwargs):
        if url.endswith("/getDefinition"):
            if definition_failure == "lookup":
                raise lookup_error
            if definition_failure == "missing-part":
                return {"body": {"definition": {"parts": []}}}
            result = _definition_response([])
            result["body"]["definition"]["parts"][0]["payload"] = (
                "!" if definition_failure == "invalid-payload" else base64.b64encode(b"{}").decode()
            )
            return result
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = failed_definition
    with pytest.raises(ItemCleanupError) as raised:
        publisher.cleanup_items([_make_item()])

    assert len(raised.value.errors) == 1
    assert "missing-source" in raised.value.errors[0][0]
    if definition_failure == "lookup":
        assert raised.value.errors[0][1] is lookup_error
    assert any(
        call.kwargs["url"].endswith("/sources/remaining-source/resume")
        for call in workspace.endpoint.invoke.call_args_list
    )
    assert not any(
        call.kwargs["url"].endswith("/sources/missing-source/resume")
        for call in workspace.endpoint.invoke.call_args_list
    )


def test_cleanup_reports_topology_failure_and_continues():
    """A topology lookup failure is surfaced without skipping recovery of later sources."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    sources = [{"id": source_id, "status": "Paused"} for source_id in ("source-1", "source-2")]
    publisher, workspace = _make_publisher(topology_sources=sources)
    publisher._paused_sources = [(EVENTSTREAM_GUID, source) for source in sources]
    invoke = workspace.endpoint.invoke.side_effect
    lookup_error = InvokeError("Topology unavailable", logging.getLogger(__name__))
    lookup_failed = False

    def fail_first_lookup(method, url, **kwargs):
        nonlocal lookup_failed
        if method == "GET" and not lookup_failed:
            lookup_failed = True
            raise lookup_error
        return invoke(method, url, **kwargs)

    workspace.endpoint.invoke.side_effect = fail_first_lookup
    with pytest.raises(ItemCleanupError) as raised:
        publisher.cleanup_items([_make_item()])

    assert raised.value.errors == [
        (f"Resume Eventstream source 'source-1' in Eventstream '{EVENTSTREAM_GUID}'", lookup_error)
    ]
    assert any(
        call.kwargs["url"].endswith("/sources/source-2/resume") for call in workspace.endpoint.invoke.call_args_list
    )


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
    assert not any(url.endswith("/sources/stopped-1/resume") for url in urls)


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
    resume_errors = {
        f"source-{index}": InvokeError(f"Resume failed for source-{index}", logging.getLogger(__name__))
        for index in range(1, source_count + 1)
    }
    _fail_resumes(workspace, resume_errors)
    publish_error = RuntimeError("Original publish failure")

    def publish():
        if publish_fails:
            raise publish_error

    expected_error = RuntimeError if publish_fails else ItemCleanupError
    with pytest.raises(expected_error) as raised, publisher._publish_lifecycle([_make_item()]):
        publish()

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
            for index, error in enumerate(resume_errors.values(), start=1)
        ]
        assert raised.value.__cause__ is resume_errors["source-1"]
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
    """Standard and bulk publishing report every resume failure without replacing primary publish errors."""
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
    resume_errors = {
        source_id: InvokeError(
            f"Resume failed for {source_id}", logging.getLogger(__name__), f"Request log: {source_id}"
        )
        for source_id in ("source-1", "source-2")
    }

    _fail_resumes(workspace, resume_errors)
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
    if (publish_fails and publish_mode == "standard") or (not publish_fails and publish_mode == "bulk"):
        post_publish.assert_called_once_with()
        if publish_mode == "bulk":
            assert "Post-publish failed" in caplog.text
    else:
        post_publish.assert_not_called()


def test_eventstream_cleanup_collects_each_resume_failure(mocker):
    """Cleanup attempts every resume, clears tracked sources, and aggregates only the failed actions."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    sources = [{"id": "source-1"}, {"id": "source-2"}, {"id": "source-3"}]
    publisher, workspace = _make_publisher(topology_sources=[{**source, "status": "Paused"} for source in sources])
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


def test_pause_timeout_survives_cleanup_wait_timeout(mocker, caplog):
    """Cleanup does not resume a transitioning source or replace the original pause timeout."""
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
    assert resume_attempts == 0
    assert retry.call_count == 2
    assert "Original pause timeout" in caplog.text


@pytest.mark.parametrize("error_message", ["Access denied", "DataSourceStatusConflict"])
def test_resume_does_not_retry_api_failures(error_message, mocker):
    """Resume attempts once, including when the source is still transitioning."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publisher, workspace = _make_publisher(
        topology_sources=[{"id": SOURCE_ID, "name": "MySource", "status": "Running"}],
    )
    error = InvokeError(error_message, logging.getLogger(__name__))
    retry = mocker.patch("fabric_cicd._items._eventstream.handle_retry")
    _fail_resumes(workspace, {SOURCE_ID: error})
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

    expected_events = ["pre", "topology:Running", "pause", "topology:Paused", "publish", "topology:Paused", "resume"]
    if (
        outcome == "success"
        or (publish_fails and publish_mode != "bulk")
        or (not publish_fails and publish_mode == "bulk")
    ):
        expected_events.append("post")
    assert events == expected_events
    if resume_fails and publish_fails:
        assert "Resume failed" in caplog.text


@pytest.mark.parametrize(
    ("outcome", "failed_batch_index"),
    [("success", None), ("publish_failure", 0), ("resume_failure", 0), ("resume_failure", 1)],
)
def test_bulk_orchestration_cleans_up_each_batch_before_refresh(
    outcome, failed_batch_index, orchestration_publisher, mocker
):
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
    unrelated = Item(type=ItemType.NOTEBOOK.value, name="UnrelatedNotebook", description="", guid="")
    workspace.repository_items[unrelated.type] = {unrelated.name: unrelated}
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
    publish_error = RuntimeError("Batch publish failed")
    resume_error = RuntimeError("Batch resume failed")
    failed_guid = [first.guid, second.guid][failed_batch_index] if failed_batch_index is not None else None

    def record_invoke(method, url, **kwargs):
        if url.endswith("/pause") or url.endswith("/resume"):
            guid = url.split("/eventstreams/")[1].split("/")[0]
            events.append((url.rsplit("/", 1)[1], guid))
            if url.endswith("/resume") and outcome == "resume_failure" and guid == failed_guid:
                raise resume_error
        return invoke(method, url, **kwargs)

    def publish(batch_items, **_kwargs):
        assert len(batch_items) == 1
        item_name, item, batch_publisher = batch_items[0]
        assert item_name == item.name
        assert batch_publisher is publisher
        events.append(("publish", item.guid))
        if outcome == "publish_failure" and item.guid == failed_guid:
            raise publish_error

    workspace.endpoint.invoke.side_effect = record_invoke
    publish_bulk = mocker.patch.object(workspace, "_publish_items", side_effect=publish)
    refresh = mocker.patch.object(workspace, "_refresh_deployed_items", side_effect=lambda: events.append("refresh"))
    post_item_states = []
    post_publish = mocker.patch.object(
        publisher,
        "post_publish_all",
        side_effect=lambda: post_item_states.append([first.skip_publish, second.skip_publish, unrelated.skip_publish]),
    )

    if outcome != "success":
        expected_error = RuntimeError if outcome == "publish_failure" else ItemCleanupError
        with pytest.raises(expected_error) as raised:
            ItemPublisher.publish_all_bulk(workspace)
        if outcome == "publish_failure":
            assert raised.value is publish_error
            post_publish.assert_not_called()
        else:
            assert raised.value.__cause__ is resume_error
            post_publish.assert_called_once_with()
            assert post_item_states == [[False, failed_batch_index == 0, False]]
        assert publish_bulk.call_count == failed_batch_index + 1
    else:
        assert ItemPublisher.publish_all_bulk(workspace) == []
        post_publish.assert_called_once_with()
        assert publish_bulk.call_count == 2

    if outcome == "success" or failed_batch_index == 1:
        refresh.assert_called_once_with()
        assert workspace._dynamic_var_cache == {"$workspace.$id": EVENTSTREAM_GUID}
    else:
        refresh.assert_not_called()

    expected_events = [("pause", first.guid), ("publish", first.guid), ("resume", first.guid)]
    if outcome == "success" or failed_batch_index == 1:
        expected_events.extend(["refresh", ("pause", second.guid), ("publish", second.guid), ("resume", second.guid)])
    assert events == expected_events
    assert not first.skip_publish
    assert not second.skip_publish
    assert not unrelated.skip_publish


@pytest.mark.parametrize("environment_post_fails", [False, True])
def test_bulk_resume_failure_runs_post_publish_only_for_completed_items(
    environment_post_fails, orchestration_publisher, mocker, caplog
):
    """Cleanup errors do not skip Environment staging submission or subsequent publishers' hooks."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    eventstream_publisher, workspace = orchestration_publisher
    workspace.bulk_publish_enabled = True
    workspace.item_type_in_scope.append(ItemType.ENVIRONMENT.value)
    environment = Item(type=ItemType.ENVIRONMENT.value, name="MyEnvironment", description="", guid=EVENTSTREAM_GUID)
    pending_environment = Item(
        type=ItemType.ENVIRONMENT.value, name="PendingEnvironment", description="", guid=EVENTSTREAM_GUID
    )
    excluded_environment = Item(
        type=ItemType.ENVIRONMENT.value, name="ExcludedEnvironment", description="", guid=EVENTSTREAM_GUID
    )
    workspace.repository_items[ItemType.ENVIRONMENT.value] = {
        environment.name: environment,
        pending_environment.name: pending_environment,
        excluded_environment.name: excluded_environment,
    }
    pending_library = Item(
        type=ItemType.VARIABLE_LIBRARY.value, name="PendingLibrary", description="", guid=EVENTSTREAM_GUID
    )
    workspace.repository_items[pending_library.type] = {pending_library.name: pending_library}
    workspace.item_type_in_scope.append(pending_library.type)
    workspace.publish_item_name_exclude_regex = "^ExcludedEnvironment$"
    environment_publisher = EnvironmentPublisher(workspace)
    library_publisher = VariableLibraryPublisher(workspace)
    publishers = {
        ItemType.EVENTSTREAM: eventstream_publisher,
        ItemType.ENVIRONMENT: environment_publisher,
        ItemType.VARIABLE_LIBRARY: library_publisher,
    }
    mocker.patch.object(ItemPublisher, "create", side_effect=lambda item_type, _workspace: publishers[item_type])
    mocker.patch.object(environment_publisher, "pre_publish_all")
    pending_names = {pending_environment.name, pending_library.name}
    mocker.patch(
        "fabric_cicd._items._bulk_publish_dependencies.compute_publish_batches",
        side_effect=lambda items, *_args: [
            [context for context in items if context[0] not in pending_names],
            [context for context in items if context[0] in pending_names],
        ],
    )
    post_library = mocker.patch.object(library_publisher, "post_publish_all")
    events = []
    post_error = RuntimeError("Environment staging publish failed")

    def submit_environment(_workspace, item_name):
        assert pending_environment.skip_publish
        assert not pending_library.skip_publish
        events.append(("environment", item_name))
        if environment_post_fails:
            raise post_error

    submit = mocker.patch("fabric_cicd._items._environment._submit_environment_publish", side_effect=submit_environment)
    post_eventstream = mocker.patch.object(
        eventstream_publisher, "post_publish_all", side_effect=lambda: events.append("eventstream")
    )
    publish = mocker.patch.object(
        workspace, "_publish_items", side_effect=lambda *_args, **_kwargs: events.append("import")
    )
    refresh = mocker.patch.object(workspace, "_refresh_deployed_items")
    resume_error = InvokeError("Resume failed", logging.getLogger(__name__))
    _fail_resumes(workspace, {SOURCE_ID: resume_error})

    with pytest.raises(ItemCleanupError) as raised:
        ItemPublisher.publish_all_bulk(workspace)

    assert raised.value.__cause__ is resume_error
    submit.assert_called_once_with(workspace, environment.name)
    post_eventstream.assert_called_once_with()
    post_library.assert_not_called()
    publish.assert_called_once()
    refresh.assert_not_called()
    assert events == ["import", ("environment", environment.name), "eventstream"]
    assert excluded_environment.skip_publish
    assert not environment.skip_publish
    assert not pending_environment.skip_publish
    assert not pending_library.skip_publish
    post_errors = [record for record in caplog.records if record.getMessage().startswith("Post-publish hook failed")]
    if environment_post_fails:
        assert len(post_errors) == 1
        record = post_errors[0]
        assert record.levelno == logging.ERROR
        assert record.getMessage() == (
            "Post-publish hook failed for Environment after bulk cleanup failure: Environment staging publish failed"
        )
        assert record.exc_info is None
    else:
        assert not post_errors


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_wait_failure_resumes_paused_source(cleanup_fails, caplog, mocker):
    """A failed pause wait still resumes the registered source without masking the wait error."""
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    source = {"id": SOURCE_ID, "name": "MySource", "status": "Running"}
    publisher, workspace = _make_publisher(topology_sources=[source])
    wait_error = RuntimeError("Pause wait failed")
    cleanup_error = RuntimeError("Cleanup failed")

    mocker.patch("fabric_cicd._items._eventstream._wait_for_sources_paused", side_effect=wait_error)
    resume = mocker.patch(
        "fabric_cicd._items._eventstream._resume_source",
        side_effect=cleanup_error if cleanup_fails else None,
    )

    with pytest.raises(RuntimeError) as raised, publisher._publish_lifecycle([_make_item()]):
        pytest.fail("Publishing must not start after a pause wait failure")

    assert raised.value is wait_error
    resume.assert_called_once_with(workspace, EVENTSTREAM_GUID, source)
    assert publisher._paused_sources == []
    if cleanup_fails:
        assert "Cleanup failed" in caplog.text


def test_base_publish_lifecycle_is_noop():
    """The base hook is a no-op context manager so other item types are unaffected."""
    publisher = ItemPublisher.__new__(ItemPublisher)

    with publisher._publish_lifecycle([_make_item()]):
        pass


@pytest.mark.parametrize("chain_kind", ["explicit", "implicit", "suppressed", "none"])
def test_single_cleanup_failure_preserves_exception_chain(chain_kind, mocker):
    """Wrapping a resume failure preserves its cause, context, suppression, and traceback visibility."""
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


@pytest.mark.parametrize(
    ("failure_stage", "error_type"),
    [
        (None, RuntimeError),
        ("setup", RuntimeError),
        ("setup", KeyboardInterrupt),
        ("setup", SystemExit),
        ("publish", RuntimeError),
        ("publish", KeyboardInterrupt),
        ("publish", SystemExit),
    ],
)
@pytest.mark.parametrize("cleanup_fails", [False, True])
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
