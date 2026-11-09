# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Eventstream source recovery, exercised through the real standard and bulk publishing paths."""

import base64
import json
from pathlib import Path
from threading import Barrier
from types import MethodType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

from fabric_cicd import constants, publish_all_items
from fabric_cicd._common._exceptions import PublishError
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd._items._environment import EnvironmentPublisher
from fabric_cicd._items._eventstream import EventstreamPublisher
from fabric_cicd._items._semanticmodel import SemanticModelPublisher
from fabric_cicd.constants import FeatureFlag
from fabric_cicd.fabric_workspace import FabricWorkspace


class Deployment:
    def __init__(self):
        self.workspace = ws = MagicMock(spec=FabricWorkspace)
        ws.workspace_id = "ws"
        ws.base_api_url = "https://api.fabric.microsoft.com/v1/workspaces/ws"
        ws.repository_items = {}
        ws.deployed_items = {}
        ws.repository_folders = {}
        ws.responses = None
        ws.items_to_include = None
        ws.item_type_in_scope = ["Eventstream", "Environment", "Notebook", "SemanticModel"]
        ws.bulk_publish_enabled = False
        ws.contains_param_item_vars = False
        ws.publish_item_name_exclude_regex = None
        ws.publish_folder_path_exclude_regex = None
        ws.publish_folder_path_to_include = None
        ws._dynamic_var_cache = {}
        ws.endpoint = MagicMock()
        for name in (
            "_publish_item",
            "_publish_items",
            "_prepare_bulk_item_parts",
            "_apply_publish_filters",
            "_apply_folder_path_filters",
        ):
            getattr(ws, name).side_effect = MethodType(getattr(FabricWorkspace, name), ws)
        ws.endpoint.invoke.side_effect = self.invoke
        self.sources = {}
        self.defined_sources = {}
        self.transitions = {}
        self.calls = []
        self.pause_errors = {}
        self.resume_errors = {}
        self.accept_pause_on_error = set()
        self.accept_resume_on_error = set()
        self.after_pause = {}
        self.after_resume = {}
        self.remove_on_publish = set()
        self.hide_on_publish = set()
        self.publish_error = None
        self.on_pause = None

    def add_item(self, name="stream", item_type="Eventstream", *, new=False, sources=None):
        item = Item(
            name=name,
            type=item_type,
            description="",
            guid="" if new else f"{name}-guid",
            logical_id=f"{name}-logical",
            path=Path(f"{name}.{item_type}"),
        )
        self.workspace.repository_items.setdefault(item_type, {})[name] = item
        if item_type == "Eventstream" and not new:
            self.sources[item.guid] = sources if sources is not None else [self.source("source")]
            self.defined_sources[item.guid] = {s["id"] for s in self.sources[item.guid]}
        return item

    @staticmethod
    def source(source_id, status="Running", source_type="AzureEventHub"):
        return {"id": source_id, "name": source_id, "status": status, "type": source_type}

    def invoke(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if url.endswith("/topology"):
            guid = url.split("/")[-2]
            result = [s.copy() for s in self.sources[guid]]
            for source in self.sources[guid]:
                states = self.transitions.get((guid, source["id"]), [])
                if states:
                    source["status"] = states.pop(0)
            return {"body": {"sources": result}}
        if url.endswith(("/pause", "/resume")):
            guid, source_id, action = url.split("/")[-4], url.split("/")[-2], url.split("/")[-1]
            source = next(s for s in self.sources[guid] if s["id"] == source_id)
            if action == "pause":
                error = self.pause_errors.get(source_id)
                if error is None or source_id in self.accept_pause_on_error:
                    source["status"] = "Pausing"
                    self.transitions[guid, source_id] = [self.after_pause.get(source_id, "Paused")]
                if self.on_pause is not None:
                    self.on_pause()
            else:
                # The test service rejects resumes sent before asynchronous pause completion.
                assert source["status"] == "Paused"
                error = self.resume_errors.get(source_id)
                if error is None or source_id in self.accept_resume_on_error:
                    source["status"] = "Resuming"
                    self.transitions[guid, source_id] = [self.after_resume.get(source_id, "Running")]
            if error is not None:
                raise error
            return {"body": {}}
        if url.endswith("/getDefinition"):
            guid = url.split("/")[-2]
            payload = json.dumps({"sources": [{"id": s} for s in self.defined_sources[guid]]})
            return {
                "body": {
                    "definition": {
                        "parts": [{"path": "eventstream.json", "payload": base64.b64encode(payload.encode()).decode()}]
                    }
                }
            }
        if "updateDefinition" in url or url.endswith("/bulkImportDefinitions"):
            if self.publish_error is not None:
                raise self.publish_error
            for guid, sources in self.sources.items():
                self.sources[guid] = [
                    s for s in sources if s["id"] not in self.remove_on_publish | self.hide_on_publish
                ]
                self.defined_sources[guid] -= self.remove_on_publish
            return {"body": {"importItemDefinitionsDetails": []}}
        if url.endswith("/items"):
            return {"body": {"id": "created-guid"}}
        if url.endswith("/workspaces/ws"):
            return {"body": {"capacityId": "capacity"}}
        if url.endswith(("/environments/", "/staging/publish?beta=False")):
            return {"body": {"value": []}}
        pytest.fail(f"Unexpected API call: {method} {url}")

    def run(self, mode):
        self.workspace.bulk_publish_enabled = mode == "bulk"
        if mode == "bulk":
            return ItemPublisher.publish_all_bulk(self.workspace)
        return EventstreamPublisher(self.workspace).publish_all()

    def actions(self, suffix):
        return [(url, kwargs) for _, url, kwargs in self.calls if url.endswith(suffix)]

    def imports(self):
        return [url for _, url, _ in self.calls if "updateDefinition" in url or url.endswith("/bulkImportDefinitions")]


@pytest.fixture
def deployment(monkeypatch, mocker):
    monkeypatch.setattr(
        constants,
        "FEATURE_FLAG",
        {
            FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value,
            FeatureFlag.ENABLE_EXPERIMENTAL_FEATURES.value,
            FeatureFlag.ENABLE_BULK_PUBLISH.value,
        },
    )
    clock = SimpleNamespace(now=0)

    def sleep(delay):
        clock.now += delay

    fake_time = SimpleNamespace(time=lambda: clock.now, sleep=sleep)
    mocker.patch("fabric_cicd._items._eventstream.time", fake_time)
    mocker.patch("fabric_cicd._common._fabric_endpoint.time", fake_time)
    return Deployment()


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_pause_update_resume_and_preserve_inactive_sources(deployment, mode):
    d = deployment
    item = d.add_item(
        sources=[
            d.source("hub"),
            d.source("sample", source_type="SampleData"),
            d.source("paused", "Paused"),
            d.source("stopped", "Stopped"),
            d.source("unsupported", source_type="CustomEndpoint"),
        ]
    )
    d.run(mode)

    assert [url.split("/")[-2] for url, _ in d.actions("/pause")] == ["hub", "sample"]
    assert [kwargs["body"] for _, kwargs in d.actions("/resume")] == [
        {"startType": "WhenLastStopped"},
        {"startType": "Now"},
    ]
    assert len(d.imports()) == 1
    calls = [url for _, url, _ in d.calls]
    assert calls.index(d.actions("/pause")[-1][0]) < calls.index(d.imports()[0])
    assert calls.index(d.imports()[0]) < calls.index(d.actions("/resume")[0][0])
    assert {s["id"]: s["status"] for s in d.sources[item.guid]} == {
        "hub": "Running",
        "sample": "Running",
        "paused": "Paused",
        "stopped": "Stopped",
        "unsupported": "Running",
    }


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize("case", ["flag-off", "new", "empty"])
def test_no_source_calls_when_not_needed(deployment, mode, case, mocker):
    d = deployment
    if case != "empty":
        d.add_item(new=case == "new")
    if case == "flag-off":
        constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
        if mode == "bulk":
            mocker.patch.object(
                EventstreamPublisher, "publish_context", side_effect=AssertionError("bulk seam invoked")
            )
        else:
            mocker.patch.object(EventstreamPublisher, "_stable_sources", side_effect=AssertionError("source read"))
    d.run(mode)
    assert not any("/eventstreams/" in url for _, url, _ in d.calls)


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize("filter_name", ["selection", "name", "folder-exclude", "folder-include"])
def test_filters_precede_all_source_side_effects(deployment, mode, filter_name):
    d = deployment
    item = d.add_item()
    item.folder_path = "/excluded/child"
    if filter_name == "selection":
        d.workspace.items_to_include = ["different.Eventstream"]
    elif filter_name == "name":
        d.workspace.publish_item_name_exclude_regex = "^stream$"
    elif filter_name == "folder-exclude":
        d.workspace.publish_folder_path_exclude_regex = "^/excluded$"
    else:
        d.workspace.publish_folder_path_to_include = "/included"
    d.run(mode)
    assert item.skip_publish
    assert not d.calls


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize(
    ("initial", "settled", "pauses"),
    [
        ("Updating", "Running", 1),
        ("Resuming", "Running", 1),
        ("Pausing", "Paused", 0),
    ],
)
def test_preexisting_transitions_settle_before_publish(deployment, mode, initial, settled, pauses):
    d = deployment
    item = d.add_item(sources=[d.source("source", initial)])
    d.transitions[item.guid, "source"] = [settled]
    d.run(mode)
    assert len(d.actions("/pause")) == pauses
    assert len(d.actions("/resume")) == pauses
    assert len(d.imports()) == 1


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize("accepted", [False, True])
@pytest.mark.parametrize("failed_source", ["first", "second"])
@pytest.mark.parametrize("resume_fails", [False, True])
def test_partial_setup_failure_recovers_attempts_including_lost_responses(
    deployment, mode, accepted, failed_source, resume_fails
):
    d = deployment
    d.add_item(sources=[d.source("first"), d.source("second"), d.source("unattempted")])
    error = RuntimeError("pause response lost")
    d.pause_errors[failed_source] = error
    if accepted:
        d.accept_pause_on_error.add(failed_source)
    if resume_fails:
        d.resume_errors["first"] = RuntimeError("recovery rejected")
    with pytest.raises((PublishError, RuntimeError)) as caught:
        d.run(mode)
    primary = caught.value.errors[0][1] if mode == "standard" else caught.value
    assert primary is error
    assert not d.imports()
    attempted = ["first"] if failed_source == "first" else ["first", "second"]
    assert [url.split("/")[-2] for url, _ in d.actions("/pause")] == attempted
    expected_resumes = attempted if accepted else attempted[:-1]
    assert [url.split("/")[-2] for url, _ in d.actions("/resume")] == expected_resumes


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize("publish_fails", [False, True])
def test_attempt_all_recovery_and_preserve_primary_failure(deployment, mode, publish_fails, caplog):
    d = deployment
    d.add_item(sources=[d.source("first"), d.source("second")])
    d.resume_errors["first"] = RuntimeError("resume rejected")
    if publish_fails:
        d.publish_error = RuntimeError("definition rejected")
    with pytest.raises((PublishError, RuntimeError)) as caught:
        d.run(mode)
    if publish_fails:
        primary = caught.value.errors[0][1] if mode == "standard" else caught.value
        assert primary is d.publish_error
    else:
        assert "Recover Eventstream" in str(caught.value) + str(caught.value.additional_info)
    assert [url.split("/")[-2] for url, _ in d.actions("/resume")] == ["first", "second"]
    assert "resume rejected" in caplog.text
    assert "resume it manually" in caplog.text


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_lost_resume_response_is_reconciled(deployment, mode):
    d = deployment
    d.add_item()
    d.resume_errors["source"] = RuntimeError("response lost after resume")
    d.accept_resume_on_error.add("source")
    d.run(mode)
    assert len(d.actions("/resume")) == 1


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize(
    "source_type", ["CustomEndpoint", "FabricJobEvents", "AzureEventGridNamespace", "NewSourceType"]
)
def test_unsupported_and_unknown_sources_are_left_alone_with_warning(deployment, mode, source_type, caplog):
    d = deployment
    d.add_item(sources=[d.source("unsupported", source_type=source_type), d.source("supported")])
    d.run(mode)
    assert [url.split("/")[-2] for url, _ in d.actions("/pause")] == ["supported"]
    assert source_type in caplog.text
    assert "pause/resume support is not documented" in caplog.text


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize("confirmed_removed", [False, True])
def test_missing_source_requires_deployed_definition_confirmation(deployment, mode, confirmed_removed):
    d = deployment
    d.add_item()
    (d.remove_on_publish if confirmed_removed else d.hide_on_publish).add("source")
    if confirmed_removed:
        d.run(mode)
    else:
        with pytest.raises(PublishError):
            d.run(mode)
    assert len(d.actions("/getDefinition")) == 1
    assert not d.actions("/resume")


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_temporary_topology_gap_is_not_mistaken_for_removal(deployment, mode):
    d = deployment
    d.add_item()
    gap_returned = False

    def invoke(method, url, **kwargs):
        nonlocal gap_returned
        if d.imports() and url.endswith("/topology") and not gap_returned:
            gap_returned = True
            return {"body": {"sources": []}}
        return d.invoke(method, url, **kwargs)

    d.workspace.endpoint.invoke.side_effect = invoke
    d.run(mode)
    assert len(d.actions("/getDefinition")) == 1
    assert len(d.actions("/resume")) == 1


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_replacement_source_id_is_not_resumed(deployment, mode):
    d = deployment
    item = d.add_item()
    d.remove_on_publish.add("source")
    original_invoke = d.workspace.endpoint.invoke.side_effect

    def invoke(method, url, **kwargs):
        result = original_invoke(method, url, **kwargs)
        if "updateDefinition" in url or url.endswith("/bulkImportDefinitions"):
            d.sources[item.guid].append(d.source("replacement", "Paused"))
            d.defined_sources[item.guid].add("replacement")
        return result

    d.workspace.endpoint.invoke.side_effect = invoke
    d.run(mode)
    assert not d.actions("/resume")


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_recovery_read_failure_does_not_skip_other_sources(deployment, mode):
    d = deployment
    d.add_item(sources=[d.source("first"), d.source("second")])
    failed = False

    def invoke(method, url, **kwargs):
        nonlocal failed
        if d.imports() and url.endswith("/topology") and not failed:
            failed = True
            msg = "topology unavailable"
            raise RuntimeError(msg)
        return d.invoke(method, url, **kwargs)

    d.workspace.endpoint.invoke.side_effect = invoke
    with pytest.raises(PublishError):
        d.run(mode)
    assert [url.split("/")[-2] for url, _ in d.actions("/resume")] == ["second"]


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_polling_timeout_is_bounded_and_prevents_definition_update(deployment, mode, monkeypatch, mocker):
    d = deployment
    d.add_item(sources=[d.source("source", "Updating")])
    clock = SimpleNamespace(now=0)

    def sleep(delay):
        clock.now += delay

    fake_time = SimpleNamespace(time=lambda: clock.now, sleep=sleep)
    monkeypatch.setattr(constants, "RETRY_MAX_DURATION_SECONDS", 3)
    mocker.patch("fabric_cicd._items._eventstream.time", fake_time)
    mocker.patch("fabric_cicd._common._fabric_endpoint.time", fake_time)
    retry = mocker.patch("fabric_cicd._items._eventstream.handle_retry", side_effect=handle_retry)
    with pytest.raises(Exception, match="Maximum execution duration|Failed to publish"):
        d.run(mode)
    assert 3 <= clock.now < 23
    assert retry.call_args.kwargs["max_duration"] == 3
    assert not d.imports()
    assert not d.actions("/pause")


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_terminal_pause_failure_aborts_and_is_reported(deployment, mode, caplog):
    d = deployment
    d.add_item()
    d.after_pause["source"] = "PauseFailed"
    with pytest.raises((PublishError, RuntimeError)):
        d.run(mode)
    assert not d.imports()
    assert "PauseFailed" in caplog.text


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize("phase", ["pause", "resume"])
def test_stuck_transition_does_not_skip_other_recovery(deployment, mode, phase, monkeypatch, caplog):
    d = deployment
    d.add_item(sources=[d.source("first"), d.source("second")])
    monkeypatch.setattr(constants, "RETRY_MAX_DURATION_SECONDS", 3)
    if phase == "pause":
        d.after_pause["second"] = "Pausing"
    else:
        d.after_resume["first"] = "Resuming"
    with pytest.raises(Exception, match="Maximum execution duration|Failed to publish"):
        d.run(mode)
    assert [url.split("/")[-2] for url, _ in d.actions("/resume")] == (
        ["first"] if phase == "pause" else ["first", "second"]
    )
    assert len(d.imports()) == (0 if phase == "pause" else 1)
    assert "resume it manually" in caplog.text


@pytest.mark.parametrize("mode", ["standard", "bulk"])
def test_all_recovery_errors_are_reported(deployment, mode):
    d = deployment
    d.add_item(sources=[d.source("first"), d.source("second")])
    d.after_resume = {"first": "ResumeFailed", "second": "ResumeFailed"}
    with pytest.raises(PublishError) as caught:
        d.run(mode)
    recovery_error = caught.value.errors[0][1] if mode == "standard" else caught.value
    assert len(recovery_error.errors) == 2
    assert "first" in recovery_error.additional_info
    assert "second" in recovery_error.additional_info
    assert "ResumeFailed" in recovery_error.additional_info


def test_parallel_standard_contexts_have_independent_attempt_tracking(deployment):
    d = deployment
    d.add_item("first", sources=[d.source("first-source")])
    d.add_item("second", sources=[d.source("second-source")])
    barrier = Barrier(2, timeout=5)
    d.on_pause = barrier.wait
    d.run("standard")
    assert {url.split("/")[-2] for url, _ in d.actions("/resume")} == {"first-source", "second-source"}


def test_bulk_partial_setup_across_eventstreams_recovers_all_attempts(deployment):
    d = deployment
    d.add_item("first", sources=[d.source("first-source")])
    d.add_item("second", sources=[d.source("second-source")])
    d.add_item("third", sources=[d.source("third-source")])
    d.pause_errors["second-source"] = RuntimeError("lost pause response")
    d.accept_pause_on_error.add("second-source")
    with pytest.raises(RuntimeError, match="lost pause response"):
        d.run("bulk")
    assert not d.imports()
    assert [url.split("/")[-2] for url, _ in d.actions("/resume")] == ["first-source", "second-source"]


def stage_dependencies(deployment, mocker, edges):
    deployment.workspace.contains_param_item_vars = True
    mocker.patch("fabric_cicd._items._bulk_publish_utils.build_dynamic_variable_dependency_graph", return_value=edges)
    mocker.patch("fabric_cicd._items._bulk_publish_utils.get_async_attributes_to_wait_for", return_value={})


@pytest.mark.parametrize("failure", ["recovery", "setup", "import"])
def test_bulk_failure_finalizes_only_completed_batches(deployment, mocker, failure, caplog):
    d = deployment
    earlier = d.add_item("earlier", "Environment")
    current = d.add_item("current", "Environment")
    future = d.add_item("future", "Environment")
    d.add_item("current-stream", sources=[d.source("current-source")])
    d.add_item("future-stream", sources=[d.source("future-source")])
    d.add_item("model", "SemanticModel")
    stage_dependencies(
        d,
        mocker,
        [
            ("Environment.current", "Environment.earlier"),
            ("Eventstream.current-stream", "Environment.earlier"),
            ("SemanticModel.model", "Environment.earlier"),
            ("Environment.future", "Eventstream.current-stream"),
            ("Eventstream.future-stream", "Eventstream.current-stream"),
        ],
    )
    binding = mocker.patch.object(
        SemanticModelPublisher, "post_publish_all", side_effect=RuntimeError("binding failed")
    )
    if failure == "recovery":
        d.resume_errors["current-source"] = RuntimeError("recovery failed")
    elif failure == "setup":
        d.pause_errors["current-source"] = RuntimeError("setup failed")
    else:
        original_invoke = d.workspace.endpoint.invoke.side_effect

        def invoke(method, url, **kwargs):
            if len(d.actions("/bulkImportDefinitions")) == 1 and url.endswith("/bulkImportDefinitions"):
                d.publish_error = RuntimeError("second import failed")
            return original_invoke(method, url, **kwargs)

        d.workspace.endpoint.invoke.side_effect = invoke
    with pytest.raises((PublishError, RuntimeError)) as caught:
        d.run("bulk")
    assert "binding failed" not in str(caught.value)
    submitted = [url for url, _ in d.actions("/staging/publish?beta=False")]
    expected = [earlier, current] if failure == "recovery" else [earlier]
    assert submitted == [
        f"{d.workspace.base_api_url}/environments/{item.guid}/staging/publish?beta=False" for item in expected
    ]
    assert not future.skip_publish
    assert [url.split("/")[-2] for url, _ in d.actions("/pause")] == ["current-source"]
    assert len(d.actions("/bulkImportDefinitions")) == (1 if failure == "setup" else 2)
    assert binding.call_count == (1 if failure == "recovery" else 0)
    if failure == "recovery":
        assert "binding failed" in caplog.text


def test_multiple_dependency_batches_recover_before_next_import(deployment, mocker):
    d = deployment
    d.add_item("first", sources=[d.source("first-source")])
    d.add_item("second", sources=[d.source("second-source")])
    stage_dependencies(d, mocker, [("Eventstream.second", "Eventstream.first")])
    d.run("bulk")
    calls = [url for _, url, _ in d.calls]
    imports = [i for i, url in enumerate(calls) if url.endswith("/bulkImportDefinitions")]
    first_resume = next(i for i, url in enumerate(calls) if url.endswith("/first-source/resume"))
    second_pause = next(i for i, url in enumerate(calls) if url.endswith("/second-source/pause"))
    assert len(imports) == 2
    assert imports[0] < first_resume < second_pause < imports[1]
    assert len(d.actions("/resume")) == 2
    d.workspace._refresh_deployed_items.assert_called_once()


@pytest.mark.parametrize("flag_enabled", [False, True])
def test_bulk_without_eventstreams_never_enters_lifecycle(deployment, mocker, flag_enabled):
    d = deployment
    d.add_item("notebook", "Notebook")
    if not flag_enabled:
        constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    context = mocker.patch.object(ItemPublisher, "publish_context", side_effect=AssertionError("unexpected context"))
    d.run("bulk")
    context.assert_not_called()
    assert len(d.imports()) == 1


def test_bulk_excluded_eventstreams_leave_unrelated_publishers_on_original_path(deployment, mocker):
    d = deployment
    d.add_item()
    d.add_item("notebook", "Notebook")
    d.workspace.items_to_include = ["notebook.Notebook"]
    context = mocker.patch.object(ItemPublisher, "publish_context", side_effect=AssertionError("unexpected context"))
    d.run("bulk")
    context.assert_not_called()
    assert len(d.imports()) == 1
    assert not d.actions("/pause")


def test_flag_off_bulk_failure_keeps_existing_post_publish_behavior(deployment, mocker):
    d = deployment
    d.add_item()
    d.add_item("env", "Environment")
    constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    d.publish_error = RuntimeError("import failed")
    post = mocker.patch.object(EnvironmentPublisher, "post_publish_all")
    with pytest.raises(RuntimeError, match="import failed"):
        d.run("bulk")
    post.assert_not_called()
    assert not any("/eventstreams/" in url for _, url, _ in d.calls)


@pytest.mark.parametrize("mode", ["standard", "bulk"])
@pytest.mark.parametrize("pause_enabled", [False, True])
def test_public_publish_selects_both_modes_with_source_pause_flag(deployment, mode, pause_enabled):
    d = deployment
    d.add_item()
    if mode == "standard":
        constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_BULK_PUBLISH.value)
    if not pause_enabled:
        constants.FEATURE_FLAG.discard(FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value)
    publish_all_items(d.workspace)
    assert d.workspace.bulk_publish_enabled == (mode == "bulk")
    assert len(d.actions("/bulkImportDefinitions")) == (1 if mode == "bulk" else 0)
    assert len(d.actions("/pause")) == (1 if pause_enabled else 0)
    assert len(d.actions("/resume")) == (1 if pause_enabled else 0)


def test_bulk_finalization_attempts_remaining_publishers_after_post_failure(deployment, mocker, caplog):
    d = deployment
    d.add_item()
    d.add_item("env", "Environment")
    d.add_item("model", "SemanticModel")
    d.resume_errors["source"] = RuntimeError("resume failed")
    mocker.patch.object(EnvironmentPublisher, "post_publish_all", side_effect=RuntimeError("staging failed"))
    binding = mocker.patch.object(SemanticModelPublisher, "post_publish_all")
    with pytest.raises(PublishError) as caught:
        d.run("bulk")
    assert "resume failed" in caught.value.additional_info
    binding.assert_called_once_with()
    assert "staging failed" in caplog.text
