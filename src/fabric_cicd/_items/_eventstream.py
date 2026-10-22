# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import logging
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)


def _get_topology_sources(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str) -> list[dict]:
    """Return every source in the eventstream's topology."""
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/get-eventstream-topology
    response = fabric_workspace_obj.endpoint.invoke(
        method="GET",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/topology",
    )
    return response["body"].get("sources", [])


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
        # Identify sources that have failed to pause and raise an exception (fails the deployment run)
        failed = {source_id for source_id in pending if statuses.get(source_id) == "pausefailed"}
        if failed:
            msg = f"Failed to pause Eventstream source(s): {sorted(failed)}"
            raise Exception(msg)

        # Otherwise, sources remain pending until they pause or time out
        pending = {source_id for source_id in pending if statuses.get(source_id) != "paused"}
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


class EventstreamPublisher(ItemPublisher):
    """Publisher for Eventstream items."""

    item_type = ItemType.EVENTSTREAM.value

    @contextmanager
    def publish_context(self, items: list[Item]) -> Generator[None, None, None]:
        """Pause deployed sources before either publish path and attempt all resumes on exit."""
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value not in constants.FEATURE_FLAG:
            yield
            return

        paused_sources: list[tuple[str, dict]] = []

        def setup() -> None:
            # Skip items that do not exist yet
            for item in items:
                if not item.guid:
                    continue
                running_sources = _get_running_sources(self.fabric_workspace_obj, item.guid)
                for source in running_sources:
                    # Pause each running source and track it for later resumption
                    _pause_source(self.fabric_workspace_obj, item.guid, source)
                    paused_sources.append((item.guid, source))
                # Wait for all running sources to be paused before continuing
                if running_sources:
                    _wait_for_sources_paused(
                        self.fabric_workspace_obj, item.guid, [source["id"] for source in running_sources]
                    )

        def cleanup() -> None:
            resume_error: Optional[Exception] = None
            # Resume all previously paused sources, capturing any errors
            for eventstream_guid, source in paused_sources:
                try:
                    _resume_source(self.fabric_workspace_obj, eventstream_guid, source)
                except Exception as error:
                    logger.exception(
                        "Failed to resume Eventstream source '%s' in Eventstream '%s'",
                        source["id"],
                        eventstream_guid,
                    )
                    if resume_error is None:
                        resume_error = error
            if resume_error is not None:
                raise resume_error

        with self._publishing_lifecycle(setup, cleanup):
            yield
