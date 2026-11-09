# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import base64
import json
import logging
import time
from collections.abc import Generator
from contextlib import contextmanager

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import PublishError
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)


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
        failed = {source_id for source_id in pending if statuses.get(source_id) == "pausefailed"}
        if failed:
            msg = f"Failed to pause Eventstream source(s): {sorted(failed)}"
            raise RuntimeError(msg)
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
        """Use the same recovery context for standard updates and bulk batches."""
        with self.publish_context([item]):
            super().publish_one(item_name, item)

    @contextmanager
    def publish_context(self, items: list[Item]) -> Generator[None, None, None]:
        """Pause around standard updates or bulk imports, always recovering attempted pauses."""
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value not in constants.FEATURE_FLAG:
            yield
            return
        attempted: list[tuple[str, dict]] = []
        operation_failed = True
        try:
            self.pause_sources(items, attempted)
            yield
            operation_failed = False
        finally:
            try:
                self.resume_sources(attempted)
            except PublishError:
                if not operation_failed:
                    raise

    def pause_sources(self, items: list[Item], attempted: list[tuple[str, dict]]) -> None:
        """Pause running sources, recording attempts in the caller's operation-local list.

        The caller owns the list so partial setup can be recovered even when this
        method raises. Record before sending each request to account for lost responses.
        """
        for item in items:
            if not item.guid:
                continue
            running_sources = _get_running_sources(self.fabric_workspace_obj, item.guid)
            for source in running_sources:
                attempted.append((item.guid, source))
                _pause_source(self.fabric_workspace_obj, item.guid, source)
            if running_sources:
                _wait_for_sources_paused(
                    self.fabric_workspace_obj, item.guid, [source["id"] for source in running_sources]
                )

    def resume_sources(self, attempted: list[tuple[str, dict]]) -> None:
        """Reconcile and resume every attempted pause, reporting failures after all attempts."""
        errors: list[tuple[str, Exception]] = []
        for guid, original_source in attempted:
            source_id = original_source["id"]
            try:
                source = next(
                    (s for s in _get_topology_sources(self.fabric_workspace_obj, guid) if s["id"] == source_id), None
                )
                if source is None:
                    # Topology can lag an update. Only a deployed definition confirms removal.
                    response = self.fabric_workspace_obj.endpoint.invoke(
                        method="POST", url=f"{self.fabric_workspace_obj.base_api_url}/items/{guid}/getDefinition"
                    )
                    part = next(
                        (p for p in response["body"]["definition"]["parts"] if p["path"] == "eventstream.json"),
                        None,
                    )
                    if part is None:
                        msg = f"Eventstream '{guid}' deployed definition has no eventstream.json part"
                        raise RuntimeError(msg)
                    definition = json.loads(base64.b64decode(part["payload"], validate=True))
                    if source_id not in {s["id"] for s in definition["sources"]}:
                        logger.info(
                            "Eventstream '%s' source '%s' was removed/replaced; no resume needed.", guid, source_id
                        )
                        continue
                status = source.get("status", "").lower() if source else "missing"
                if status == "running":
                    continue
                if status == "pausing":
                    _wait_for_sources_paused(self.fabric_workspace_obj, guid, [source_id])
                elif status != "paused":
                    msg = f"Cannot recover Eventstream '{guid}' source '{source_id}' with topology status '{status}'"
                    raise RuntimeError(msg)
                _resume_source(self.fabric_workspace_obj, guid, original_source)
            except Exception as error:
                label = f"Recover Eventstream '{guid}' source '{source_id}'"
                errors.append((label, error))
                logger.error(
                    "%s failed: %s. Check the source in Fabric and resume it manually if needed.", label, error
                )
        if errors:
            raise PublishError(errors, logger) from errors[0][1]
