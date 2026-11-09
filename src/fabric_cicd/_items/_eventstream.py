# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import base64
import json
import logging
import time
from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager, nullcontext
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
        with self.publish_context([item]) or nullcontext():
            super().publish_one(item_name, item)

    def publish_context(self, items: list[Item]) -> Optional[AbstractContextManager[None]]:
        """Only existing, selected Eventstreams need source management."""
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value not in constants.FEATURE_FLAG:
            return None
        deployed_items = [item for item in items if item.guid and not item.skip_publish]
        return self._pause_and_resume(deployed_items) if deployed_items else None

    @contextmanager
    def _pause_and_resume(self, items: list[Item]) -> Generator[None, None, None]:
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
            for source in self._stable_sources(item.guid):
                if source["status"].lower() == "running":
                    attempted.append((item.guid, source))
                    self._source_action(item.guid, source, "pause")
                    self._wait_for_source_paused(item.guid, source["id"])

    def resume_sources(self, attempted: list[tuple[str, dict]]) -> None:
        """Reconcile and resume every attempted pause, reporting failures after all attempts."""
        errors: list[tuple[str, Exception]] = []
        for guid, original_source in attempted:
            source_id = original_source["id"]
            try:
                source = next((s for s in self._stable_sources(guid, source_id) if s["id"] == source_id), None)
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
                self._source_action(guid, original_source, "resume")
            except Exception as error:
                label = f"Recover Eventstream '{guid}' source '{source_id}'"
                errors.append((label, error))
                logger.error(
                    "%s failed: %s. Check the source in Fabric and resume it manually if needed.", label, error
                )
        if errors:
            raise PublishError(errors, logger) from errors[0][1]

    def _stable_sources(self, guid: str, source_id: Optional[str] = None) -> list[dict]:
        """Wait for transitions without taking ownership of sources paused by someone else."""
        start_time = time.time()
        attempt = 0
        while True:
            sources = self._get_sources(guid)
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

    def _get_sources(self, guid: str) -> list[dict]:
        response = self.fabric_workspace_obj.endpoint.invoke(
            method="GET", url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{guid}/topology"
        )
        return response["body"]["sources"]

    def _wait_for_source_paused(self, guid: str, source_id: str) -> None:
        """Wait for Paused, tolerating stale Running or missing topology after an accepted pause."""
        start_time = time.time()
        attempt = 0
        while True:
            status = next((s["status"].lower() for s in self._get_sources(guid) if s["id"] == source_id), "missing")
            if status == "paused":
                return
            if status == "pausefailed":
                msg = f"Failed to pause Eventstream '{guid}' source '{source_id}'"
                raise RuntimeError(msg)
            handle_retry(
                attempt=attempt,
                base_delay=2,
                response_retry_after=20,
                prepend_message=f"Waiting for Eventstream '{guid}' source '{source_id}' to pause.",
                max_duration=constants.RETRY_MAX_DURATION_SECONDS,
                start_time=start_time,
            )
            attempt += 1

    def _source_action(self, guid: str, source: dict, action: str) -> None:
        logger.info("%sEventstream '%s': %s source '%s'", constants.INDENT, guid, action, source["id"])
        kwargs = {}
        if action == "resume":
            kwargs["body"] = {"startType": "Now" if source.get("type") == "SampleData" else "WhenLastStopped"}
        self.fabric_workspace_obj.endpoint.invoke(
            method="POST",
            url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{guid}/sources/{source['id']}/{action}",
            **kwargs,
        )
