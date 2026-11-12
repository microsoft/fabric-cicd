# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import logging
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import PublishError
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)


@dataclass
class _SourcePauseAttempt:
    """Track a pause request and whether its API call returned successfully."""

    guid: str
    source: dict
    acknowledged: bool = False


def _get_topology_sources(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str) -> list[dict]:
    """Return every source in the eventstream's topology."""
    response = fabric_workspace_obj.endpoint.invoke(
        method="GET",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/topology",
    )
    return response["body"].get("sources", [])


def _get_running_sources(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str) -> list[dict]:
    """Wait for source transitions to finish, then return the currently running sources."""
    start_time = time.time()
    attempt = 0
    while True:
        sources = _get_topology_sources(fabric_workspace_obj, eventstream_guid)
        if not any(source.get("status", "").lower() in {"updating", "pausing", "resuming"} for source in sources):
            return [source for source in sources if source.get("status", "").lower() == "running"]
        # Wait between topology checks and stop at the configured timeout
        handle_retry(
            attempt=attempt,
            base_delay=2,
            response_retry_after=20,
            prepend_message=f"{constants.INDENT}Waiting for Eventstream source transition(s) to finish.",
            max_duration=constants.RETRY_MAX_DURATION_SECONDS,
            start_time=start_time,
        )
        attempt += 1


def _wait_for_sources_paused(
    fabric_workspace_obj: FabricWorkspace, eventstream_guid: str, source_ids: list[str]
) -> None:
    """Wait for asynchronous pauses to finish before publishing or resuming."""
    pending = set(source_ids)
    start_time = time.time()
    attempt = 0
    while pending:
        statuses = {
            source["id"]: source.get("status", "").lower()
            for source in _get_topology_sources(fabric_workspace_obj, eventstream_guid)
        }
        # The API's PauseFailed status is a terminal failure; stop waiting
        failed = {source_id for source_id in pending if statuses.get(source_id) == "pausefailed"}
        if failed:
            msg = f"Failed to pause Eventstream source(s): {sorted(failed)}"
            raise RuntimeError(msg)
        # Keep waiting for sources not yet confirmed paused, including missing sources
        pending = {source_id for source_id in pending if statuses.get(source_id) != "paused"}
        if not pending:
            return
        # Bound polling by the configured timeout
        handle_retry(
            attempt=attempt,
            base_delay=2,
            response_retry_after=20,
            prepend_message=f"{constants.INDENT}Waiting for Eventstream source(s) to pause.",
            max_duration=constants.RETRY_MAX_DURATION_SECONDS,
            start_time=start_time,
        )
        attempt += 1


def _pause_source(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str, source: dict) -> None:
    """Pause a single eventstream source."""
    logger.info("%sPausing Eventstream source '%s'", constants.INDENT, source.get("name", source["id"]))
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/sources/{source['id']}/pause",
    )


def _resume_source(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str, source: dict) -> None:
    """Resume Sample Data from now and other sources from where they last stopped."""
    logger.info("%sResuming Eventstream source '%s'", constants.INDENT, source.get("name", source["id"]))
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/sources/{source['id']}/resume",
        body={"startType": "Now" if source.get("type") == "SampleData" else "WhenLastStopped"},
    )


class EventstreamPublisher(ItemPublisher):
    """Publisher for Eventstream items."""

    item_type = ItemType.EVENTSTREAM.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish an Eventstream, optionally pausing sources and attempting to resume them even on failure."""
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value in constants.FEATURE_FLAG:
            with self.publish_context([item]):
                super().publish_one(item_name, item)
        else:
            super().publish_one(item_name, item)

    @contextmanager
    def publish_context(self, items: list[Item]) -> Generator[None, None, None]:
        """Pause running sources, publish once, and always attempt to resume paused sources."""
        attempted: list[_SourcePauseAttempt] = []
        pause_or_publish_failed = True
        try:
            self.pause_sources(items, attempted)
            yield
            pause_or_publish_failed = False
        finally:
            try:
                self.resume_sources(attempted)
            except PublishError:
                # Recovery errors are logged; keep the original pause or publish error if one exists
                if not pause_or_publish_failed:
                    raise

    def pause_sources(self, items: list[Item], attempted: list[_SourcePauseAttempt]) -> None:
        """Pause running sources and track which ones may need resuming.

        Add each source to the caller's list before requesting its pause, so the caller
        can attempt recovery even if this method fails or a pause response is lost.
        """
        for item in items:
            # Skip items not yet deployed
            if not item.guid:
                continue
            running_sources = _get_running_sources(self.fabric_workspace_obj, item.guid)
            for source in running_sources:
                # Track before requesting a pause so recovery can handle lost responses
                pause_attempt = _SourcePauseAttempt(item.guid, source)
                attempted.append(pause_attempt)
                _pause_source(self.fabric_workspace_obj, item.guid, source)
                pause_attempt.acknowledged = True
            if running_sources:
                # Wait for all asynchronous pauses to finish before publishing
                _wait_for_sources_paused(
                    self.fabric_workspace_obj, item.guid, [source["id"] for source in running_sources]
                )

    def resume_sources(self, attempted: list[_SourcePauseAttempt]) -> None:
        """Attempt to resume each source whose pause was attempted, then report any failures."""
        errors: list[tuple[str, Exception]] = []
        for pause_attempt in attempted:
            guid, original_source = pause_attempt.guid, pause_attempt.source
            source_id = original_source["id"]
            try:
                # Check the current state; an attempted pause may not have succeeded
                source = next(
                    (s for s in _get_topology_sources(self.fabric_workspace_obj, guid) if s["id"] == source_id), None
                )
                status = source.get("status", "").lower() if source else "missing"
                if status == "running" and pause_attempt.acknowledged:
                    continue
                if status == "pausing" or (not pause_attempt.acknowledged and status in {"running", "missing"}):
                    # A lost response can precede a visible transition; poll before declaring recovery complete
                    _wait_for_sources_paused(self.fabric_workspace_obj, guid, [source_id])
                elif status != "paused":
                    msg = f"Cannot recover Eventstream '{guid}' source '{source_id}' with topology status '{status}'"
                    raise RuntimeError(msg)
                _resume_source(self.fabric_workspace_obj, guid, original_source)
            except Exception as error:
                # Keep recovering other sources even if this one fails
                label = f"Recover Eventstream '{guid}' source '{source_id}'"
                errors.append((label, error))
                logger.error(
                    "%s failed: %s. Check the source in Fabric and resume it manually if needed.", label, error
                )
        if errors:
            # Report all recovery failures after every source has been checked
            raise PublishError(errors, logger) from errors[0][1]
