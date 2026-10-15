# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import logging
import time
from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)

# Source statuses that indicate the (asynchronous) pause operation has settled
_PAUSE_TERMINAL_STATUSES = frozenset({"paused", "pausefailed"})


def _get_topology_sources(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str) -> list[dict]:
    """Return every source in the eventstream's topology."""
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/get-eventstream-topology
    response = fabric_workspace_obj.endpoint.invoke(
        method="GET",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/topology",
    )
    return response["body"].get("sources", []) or []


def _get_running_sources(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str) -> list[dict]:
    """Return every currently running source in the eventstream's topology."""
    return [
        source
        for source in _get_topology_sources(fabric_workspace_obj, eventstream_guid)
        if source.get("status", "").lower() == "running"
    ]


def _wait_for_sources_paused(
    fabric_workspace_obj: FabricWorkspace, eventstream_guid: str, source_ids: list[str]
) -> None:
    """
    Poll until each source reaches a terminal pause status.

    Pausing is asynchronous, and resume requests fail while a source is still transitioning.
    """
    pending = set(source_ids)
    start_time = time.time()
    attempt = 0
    while pending:
        statuses = {
            source["id"]: source.get("status", "").lower()
            for source in _get_topology_sources(fabric_workspace_obj, eventstream_guid)
        }
        # Keep waiting only on sources that are still present and still transitioning
        pending = {
            source_id
            for source_id in pending
            if source_id in statuses and statuses[source_id] not in _PAUSE_TERMINAL_STATUSES
        }
        if not pending:
            return
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
    source_name = source.get("name", source["id"])
    logger.info(f"{constants.INDENT}Pausing Eventstream source '{source_name}'")
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/pause-eventstream-source
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/sources/{source['id']}/pause",
    )


def _resume_source(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str, source: dict) -> None:
    """Resume a single eventstream source from where it was last stopped."""
    source_name = source.get("name", source["id"])
    logger.info(f"{constants.INDENT}Resuming Eventstream source '{source_name}'")
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/resume-eventstream-source
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/sources/{source['id']}/resume",
        body={"startType": "WhenLastStopped"},
    )


@contextmanager
def _paused_eventstream_sources(
    fabric_workspace_obj: FabricWorkspace, items: list[Item]
) -> Generator[None, None, None]:
    """
    Handles source pausing for both publish paths (standard and bulk) via EventstreamPublisher.publish_items_context.
    It runs only when enable_eventstream_source_pause is set, skips undeployed Eventstreams, and
    resumes sources in a finally block.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        items: The eventstream Item objects being published.
    """
    # List of (eventstream_guid, source) tuples
    paused_sources = []
    try:
        # Only proceed if the feature flag for pausing eventstream sources is enabled
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value in constants.FEATURE_FLAG:
            for item in items:
                # Skip items that are not yet deployed
                if not item.guid:
                    continue
                # Get the running sources for the deployed eventstream and pause them
                running_sources = _get_running_sources(fabric_workspace_obj, item.guid)
                for source in running_sources:
                    _pause_source(fabric_workspace_obj, item.guid, source)
                    paused_sources.append((item.guid, source))
                # Wait until the sources settle so the later resume is not rejected while still 'Pausing'
                if running_sources:
                    _wait_for_sources_paused(
                        fabric_workspace_obj, item.guid, [source["id"] for source in running_sources]
                    )
        yield
    finally:
        # Resume all previously paused sources
        for eventstream_guid, source in paused_sources:
            _resume_source(fabric_workspace_obj, eventstream_guid, source)


class EventstreamPublisher(ItemPublisher):
    """Publisher for Eventstream items."""

    item_type = ItemType.EVENTSTREAM.value

    def publish_items_context(self, items: list[Item]) -> AbstractContextManager[None]:
        """Return a context manager that pauses running sources while Eventstreams are published."""
        return _paused_eventstream_sources(self.fabric_workspace_obj, items)
