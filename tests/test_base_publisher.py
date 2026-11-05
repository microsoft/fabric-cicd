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


def test_publish_error_names_post_publish_failure_when_both_fail():
    hook_error = _post_publish_failure()
    publisher = _StubPublisher(["LakehouseA"], hook_error)

    with pytest.raises(PublishError) as exc_info:
        publisher.publish_all()

    error = exc_info.value
    assert error.errors == [("LakehouseA", publisher.publish_failures["LakehouseA"])]
    assert error.post_publish_error is hook_error
    assert error.__cause__ is hook_error
    assert str(error) == (
        f"Failed to publish 1 item(s): ['LakehouseA']. The post-publish step also failed: {POST_PUBLISH_MESSAGE}"
    )


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
