# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import base64
import json
import logging
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional

from fabric_cicd import constants
from fabric_cicd._common._exceptions import PublishError
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)

_TRANSITIONAL_STATUSES = {"updating", "pausing", "resuming"}


class EventstreamPublisher(ItemPublisher):
    """Publisher for Eventstream items."""

    item_type = ItemType.EVENTSTREAM.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Use the same recovery context for standard updates and bulk batches."""
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value in constants.FEATURE_FLAG and (
            item.skip_publish or self.fabric_workspace_obj._apply_publish_filters(item, item_name, self.item_type)
        ):
            return
        with self.publish_context([item]):
            super().publish_one(item_name, item)

    @contextmanager
    def publish_context(self, items: list[Item]) -> Generator[None, None, None]:
        """Pause around standard updates or bulk imports, always recovering attempted pauses."""
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value not in constants.FEATURE_FLAG:
            yield
            return
        deployed_items = [item for item in items if item.guid and not item.skip_publish]
        if not deployed_items:
            yield
            return
        attempted: list[tuple[str, dict]] = []
        operation_failed = True
        try:
            self.pause_sources(deployed_items, attempted)
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
            for source in self._wait_for_source_transitions(item.guid):
                if source["status"].lower() != "running":
                    continue
                attempted.append((item.guid, source))
                logger.info("%sEventstream '%s': pause source '%s'", constants.INDENT, item.guid, source["id"])
                self.fabric_workspace_obj.endpoint.invoke(
                    method="POST",
                    url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{item.guid}/sources/{source['id']}/pause",
                )
                start_time = time.time()
                attempt = 0
                while True:
                    response = self.fabric_workspace_obj.endpoint.invoke(
                        method="GET",
                        url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{item.guid}/topology",
                    )
                    status = next(
                        (s["status"].lower() for s in response["body"]["sources"] if s["id"] == source["id"]),
                        "missing",
                    )
                    if status == "paused":
                        break
                    if status == "pausefailed":
                        msg = f"Failed to pause Eventstream '{item.guid}' source '{source['id']}'"
                        raise RuntimeError(msg)
                    # Accepted pauses can still appear Running or missing in stale topology.
                    handle_retry(
                        attempt=attempt,
                        base_delay=2,
                        response_retry_after=20,
                        prepend_message=f"Waiting for Eventstream '{item.guid}' source '{source['id']}' to pause.",
                        max_duration=constants.RETRY_MAX_DURATION_SECONDS,
                        start_time=start_time,
                    )
                    attempt += 1

    def resume_sources(self, attempted: list[tuple[str, dict]]) -> None:
        """Reconcile and resume every attempted pause, reporting failures after all attempts."""
        errors: list[tuple[str, Exception]] = []
        for guid, original_source in attempted:
            source_id = original_source["id"]
            try:
                source = next(
                    (s for s in self._wait_for_source_transitions(guid, source_id) if s["id"] == source_id), None
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
                status = source["status"].lower() if source else "missing"
                if status == "running":
                    continue
                if status != "paused":
                    msg = f"Cannot recover Eventstream '{guid}' source '{source_id}' with topology status '{status}'"
                    raise RuntimeError(msg)
                logger.info("%sEventstream '%s': resume source '%s'", constants.INDENT, guid, source_id)
                self.fabric_workspace_obj.endpoint.invoke(
                    method="POST",
                    url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{guid}/sources/{source_id}/resume",
                    body={"startType": "Now" if original_source.get("type") == "SampleData" else "WhenLastStopped"},
                )
            except Exception as error:
                label = f"Recover Eventstream '{guid}' source '{source_id}'"
                errors.append((label, error))
                logger.error(
                    "%s failed: %s. Check the source in Fabric and resume it manually if needed.", label, error
                )
        if errors:
            raise PublishError(errors, logger) from errors[0][1]

    def _wait_for_source_transitions(self, guid: str, source_id: Optional[str] = None) -> list[dict]:
        """Wait for transitions without taking ownership of sources paused by someone else."""
        start_time = time.time()
        attempt = 0
        while True:
            response = self.fabric_workspace_obj.endpoint.invoke(
                method="GET", url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{guid}/topology"
            )
            sources = response["body"]["sources"]
            if not any(
                source["status"].lower() in _TRANSITIONAL_STATUSES
                for source in sources
                if source_id is None or source["id"] == source_id
            ):
                return sources
            handle_retry(
                attempt=attempt,
                base_delay=2,
                response_retry_after=20,
                prepend_message=f"Waiting for Eventstream '{guid}' source transitions.",
                max_duration=constants.RETRY_MAX_DURATION_SECONDS,
                start_time=start_time,
            )
            attempt += 1
