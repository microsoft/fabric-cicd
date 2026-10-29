# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test error handling in ItemPublisher.publish_all()."""

import json
import logging
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from fabric_cicd import constants
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InvokeError, PublishError
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher, ParallelConfig
from fabric_cicd._items._lakehouse import LakehousePublisher
from fabric_cicd.constants import FeatureFlag
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)

POST_PUBLISH_MESSAGE = "Failed to publish 'sc_sales' for lakehouse LakehouseB"


class _StubPublisher(ItemPublisher):
    """Publishes two items, fails the ones in failing_items, then raises hook_error from post_publish_all()."""

    item_type = "Lakehouse"
    parallel_config = ParallelConfig(enabled=False)

    def __init__(self, failing_items: list[str], hook_error: Optional[Exception]) -> None:
        workspace = MagicMock(spec=FabricWorkspace)
        workspace.repository_items = {}
        workspace._apply_publish_filters.return_value = False
        super().__init__(workspace)
        self.failing_items = failing_items
        self.hook_error = hook_error
        self.publish_failures: dict[str, Exception] = {}

    def get_items_to_publish(self) -> dict:
        return {"LakehouseA": MagicMock(), "LakehouseB": MagicMock()}

    def publish_one(self, item_name: str, _item: object) -> None:
        if item_name in self.failing_items:
            self.publish_failures[item_name] = RuntimeError("definition rejected")
            raise self.publish_failures[item_name]

    def post_publish_all(self) -> None:
        if self.hook_error is not None:
            raise self.hook_error


def _post_publish_failure() -> FailedPublishedItemStatusError:
    return FailedPublishedItemStatusError(POST_PUBLISH_MESSAGE, logger)


@pytest.fixture
def shortcut_publish_enabled():
    """Enable the ENABLE_SHORTCUT_PUBLISH feature flag for the duration of a test."""
    original_flags = constants.FEATURE_FLAG.copy()
    constants.FEATURE_FLAG.add(FeatureFlag.ENABLE_SHORTCUT_PUBLISH.value)
    yield
    constants.FEATURE_FLAG.clear()
    constants.FEATURE_FLAG.update(original_flags)


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_publish_error_names_post_publish_failure_when_both_fail(cleanup_fails, mocker, caplog):
    hook_error = _post_publish_failure()
    publisher = _StubPublisher(["LakehouseA"], hook_error)
    if cleanup_fails:
        mocker.patch.object(publisher, "cleanup_items", side_effect=RuntimeError("cleanup failed"))

    with pytest.raises(PublishError) as exc_info:
        publisher.publish_all()

    error = exc_info.value
    assert error.errors == [("LakehouseA", publisher.publish_failures["LakehouseA"])]
    assert error.post_publish_error is hook_error
    assert error.__cause__ is hook_error
    assert str(error) == (
        f"Failed to publish 1 item(s): ['LakehouseA']. The post-publish step also failed: {POST_PUBLISH_MESSAGE}"
    )
    if cleanup_fails:
        assert "Failed to clean up after item publishing" in caplog.text


def test_publish_error_keeps_additional_info_of_post_publish_failure():
    # FabricEndpoint.invoke() puts the request and response log in additional_info
    request_log = "URL: https://api.fabric.microsoft.com/v1/workspaces/ws/items/b-guid/shortcuts\nStatus: 403"
    hook_error = InvokeError(Exception("Operation failed. Error Code: Forbidden"), logger, request_log)
    publisher = _StubPublisher(["LakehouseA"], hook_error)

    with pytest.raises(PublishError) as exc_info:
        publisher.publish_all()

    assert exc_info.value.additional_info == (
        f"\n--- LakehouseA ---\ndefinition rejected\n\n--- post_publish_all ---\n{request_log}"
    )


def test_post_publish_failure_is_raised_unchanged_when_no_item_fails():
    hook_error = _post_publish_failure()
    publisher = _StubPublisher([], hook_error)

    with pytest.raises(FailedPublishedItemStatusError) as exc_info:
        publisher.publish_all()

    assert exc_info.value is hook_error


def test_publish_error_is_unchanged_when_post_publish_succeeds():
    publisher = _StubPublisher(["LakehouseA"], None)

    with pytest.raises(PublishError) as exc_info:
        publisher.publish_all()

    error = exc_info.value
    assert error.errors == [("LakehouseA", publisher.publish_failures["LakehouseA"])]
    assert error.post_publish_error is None
    assert str(error) == "Failed to publish 1 item(s): ['LakehouseA']"


def test_non_custom_post_publish_exception_is_combined_with_item_failures():
    # A malformed shortcuts.metadata.json raises JSONDecodeError in the shortcut hook
    hook_error = json.JSONDecodeError("Expecting value", "", 0)
    publisher = _StubPublisher(["LakehouseA"], hook_error)

    with pytest.raises(PublishError) as exc_info:
        publisher.publish_all()

    error = exc_info.value
    assert error.errors == [("LakehouseA", publisher.publish_failures["LakehouseA"])]
    assert error.post_publish_error is hook_error
    assert error.__cause__ is hook_error


def test_non_custom_post_publish_exception_is_raised_unchanged_when_no_item_fails():
    hook_error = KeyError("path")
    publisher = _StubPublisher([], hook_error)

    with pytest.raises(KeyError) as exc_info:
        publisher.publish_all()

    assert exc_info.value is hook_error


def test_lakehouse_publish_error_names_failed_shortcut(shortcut_publish_enabled):  # noqa: ARG001
    """
    Scenario from #1174: LakehouseA fails to publish and a shortcut of LakehouseB fails.
    The raised PublishError must name both failures.
    """
    lakehouse_a = Item(type="Lakehouse", name="LakehouseA", description="", guid="")  # creation failed
    lakehouse_b = Item(type="Lakehouse", name="LakehouseB", description="", guid="b-guid")

    workspace = MagicMock(spec=FabricWorkspace)
    workspace.repository_items = {"Lakehouse": {"LakehouseA": lakehouse_a, "LakehouseB": lakehouse_b}}
    workspace.items_to_include = None
    workspace._apply_publish_filters.return_value = False
    publisher = LakehousePublisher(workspace)

    publish_failure = RuntimeError("definition rejected")

    def fake_publish_one(item_name, _item):
        if item_name == "LakehouseA":
            raise publish_failure

    def fake_shortcut_publish_all(self_inner):
        msg = f"Failed to publish 'sc_sales' for lakehouse {self_inner.item_obj.name}"
        raise FailedPublishedItemStatusError(msg, logger)

    with (
        patch.object(LakehousePublisher, "publish_one", side_effect=fake_publish_one),
        patch("fabric_cicd._items._lakehouse.ShortcutPublisher.publish_all", fake_shortcut_publish_all),
        pytest.raises(PublishError) as exc_info,
    ):
        publisher.publish_all()

    error = exc_info.value
    assert error.errors == [("LakehouseA", publish_failure)]
    assert isinstance(error.post_publish_error, FailedPublishedItemStatusError)
    assert str(error) == (
        f"Failed to publish 1 item(s): ['LakehouseA']. The post-publish step also failed: {POST_PUBLISH_MESSAGE}"
    )


@pytest.mark.parametrize("publish_mode", ["parallel", "sequential", "ordered"])
@pytest.mark.parametrize("publish_fails", [False, True])
@pytest.mark.parametrize("post_publish_fails", [False, True])
def test_cleanup_failure_is_reported_without_changing_post_publish_behavior(
    publish_mode, publish_fails, post_publish_fails, mocker, caplog
):
    publisher = _StubPublisher(["LakehouseA"] if publish_fails else [], None)
    mocker.patch.object(
        _StubPublisher,
        "parallel_config",
        ParallelConfig(
            enabled=publish_mode == "parallel",
            ordered_items_func=(lambda _publisher: ["LakehouseA", "LakehouseB"]) if publish_mode == "ordered" else None,
        ),
    )
    request_log = "URL: https://api.fabric.microsoft.com/resume\nStatus: 403"
    cleanup_error = InvokeError("Resume forbidden", logger, request_log)
    mocker.patch.object(publisher, "cleanup_items", side_effect=cleanup_error)
    post_error = _post_publish_failure()
    post_publish = mocker.patch.object(
        publisher, "post_publish_all", side_effect=post_error if post_publish_fails else None
    )

    with pytest.raises(PublishError if publish_fails else InvokeError) as raised:
        publisher.publish_all()

    if not publish_fails:
        assert raised.value is cleanup_error
        post_publish.assert_not_called()
        return

    error = raised.value
    assert error.errors == [("LakehouseA", publisher.publish_failures["LakehouseA"])]
    assert error.post_publish_error is (post_error if post_publish_fails else None)
    assert "Resume forbidden" in caplog.text
    assert request_log not in error.additional_info
    if post_publish_fails:
        assert error.__cause__ is post_error
    else:
        assert error.__cause__ is None
    post_publish.assert_called_once_with()


@pytest.mark.parametrize("error_type", [RuntimeError, KeyboardInterrupt])
def test_setup_and_cleanup_failures_preserve_the_setup_error(error_type, mocker):
    publisher = _StubPublisher([], None)
    setup_error = error_type("Setup failed")
    cleanup_error = RuntimeError("Cleanup failed")
    mocker.patch.object(publisher, "setup_items", side_effect=setup_error)
    cleanup = mocker.patch.object(publisher, "cleanup_items", side_effect=cleanup_error)
    post_publish = mocker.patch.object(publisher, "post_publish_all")

    with pytest.raises(error_type) as raised:
        publisher.publish_all()

    assert raised.value is setup_error
    cleanup.assert_called_once()
    post_publish.assert_not_called()


@pytest.mark.parametrize("operation_fails", [False, True])
@pytest.mark.parametrize("item_errors", [False, True])
def test_batch_lifecycle_preserves_primary_error_and_attempts_all_cleanup(operation_fails, item_errors, mocker, caplog):
    first = _StubPublisher([], None)
    second = _StubPublisher([], None)
    second.item_type = "Eventstream"
    first_cleanup_error = RuntimeError("First cleanup failed")
    second_cleanup_error = RuntimeError("Second cleanup failed")
    events = []

    def fail_cleanup(publisher_name, error):
        events.append(publisher_name)
        raise error

    mocker.patch.object(first, "cleanup_items", side_effect=lambda _items: fail_cleanup("first", first_cleanup_error))
    mocker.patch.object(
        second, "cleanup_items", side_effect=lambda _items: fail_cleanup("second", second_cleanup_error)
    )
    item_failure = RuntimeError("Definition rejected")
    operation_error = PublishError([("LakehouseA", item_failure)], logger) if item_errors else item_failure
    batch = [("LakehouseA", MagicMock(), first), ("Stream", MagicMock(), second)]

    def publish_batch():
        with ItemPublisher._batch_publish_lifecycle(batch):
            if operation_fails:
                raise operation_error

    expected_error = PublishError if operation_fails and item_errors else RuntimeError
    with pytest.raises(expected_error) as raised:
        publish_batch()

    assert events == ["second", "first"]
    assert raised.value is (operation_error if operation_fails else second_cleanup_error)
    assert "First cleanup failed" in caplog.text
    if operation_fails:
        assert "Second cleanup failed" in caplog.text


@pytest.mark.parametrize("failing_publisher", ["first", "second"])
def test_single_batch_cleanup_failure_is_unchanged(failing_publisher, mocker):
    first = _StubPublisher([], None)
    second = _StubPublisher([], None)
    error = RuntimeError("Cleanup failed")
    events = []

    def cleanup(name):
        events.append(name)
        if name == failing_publisher:
            raise error

    mocker.patch.object(first, "cleanup_items", side_effect=lambda _items: cleanup("first"))
    mocker.patch.object(second, "cleanup_items", side_effect=lambda _items: cleanup("second"))
    batch = [("LakehouseA", MagicMock(), first), ("LakehouseB", MagicMock(), second)]

    def publish_batch():
        with ItemPublisher._batch_publish_lifecycle(batch):
            events.append("publish")

    with pytest.raises(RuntimeError) as raised:
        publish_batch()

    assert raised.value is error
    assert events == ["publish", "second", "first"]


def test_batch_setup_failure_survives_cleanup_failures(mocker, caplog):
    first = _StubPublisher([], None)
    second = _StubPublisher([], None)
    second.item_type = "Eventstream"
    setup_error = RuntimeError("Second setup failed")
    first_cleanup_error = RuntimeError("First cleanup failed")
    second_cleanup_error = RuntimeError("Second cleanup failed")
    mocker.patch.object(second, "setup_items", side_effect=setup_error)
    first_cleanup = mocker.patch.object(first, "cleanup_items", side_effect=first_cleanup_error)
    second_cleanup = mocker.patch.object(second, "cleanup_items", side_effect=second_cleanup_error)
    batch = [("LakehouseA", MagicMock(), first), ("Stream", MagicMock(), second)]
    publish = mocker.Mock()

    def publish_batch():
        with ItemPublisher._batch_publish_lifecycle(batch):
            publish()

    with pytest.raises(RuntimeError) as raised:
        publish_batch()

    assert raised.value is setup_error
    assert "First cleanup failed" in caplog.text
    assert "Second cleanup failed" in caplog.text
    first_cleanup.assert_called_once()
    second_cleanup.assert_called_once()
    publish.assert_not_called()
