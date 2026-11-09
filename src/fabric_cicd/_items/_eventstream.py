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

# https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/pause-resume-data-streams
_PAUSABLE_SOURCE_TYPES = {
    "AmazonKinesis",
    "AmazonMSKKafka",
    "ApacheKafka",
    "AzureCosmosDBCDC",
    "AzureDataExplorer",
    "AzureEventHub",
    "AzureEventHubExtended",
    "AzureIoTHub",
    "AzureIoTHubExtended",
    "AzureServiceBus",
    "AzureSQLDBCDC",
    "AzureSQLMIDBCDC",
    "ConfluentCloud",
    "GooglePubSub",
    "Http",
    "MongoDBCDC",
    "Mqtt",
    "MySQLCDC",
    "OracleDBCDC",
    "PostgreSQLCDC",
    "RealTimeWeather",
    "SampleData",
    "SolacePubSub",
    "SQLServerOnVMDBCDC",
}
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
            for item in items:
                sources = self._stable_sources(item.guid)
                for source in sources:
                    if source["status"].lower() != "running":
                        continue
                    if source["type"] not in _PAUSABLE_SOURCE_TYPES:
                        logger.warning(
                            "Not pausing Eventstream source '%s' (%s): pause/resume support is not documented. "
                            "Fabric will validate the definition update.",
                            source["id"],
                            source["type"],
                        )
                        continue
                    # The service can accept a pause even if the client loses its response.
                    attempted.append((item.guid, source))
                    self._source_action(item.guid, source, "pause")
                    paused = next(
                        (s for s in self._stable_sources(item.guid, source["id"]) if s["id"] == source["id"]),
                        None,
                    )
                    if paused is None or paused["status"].lower() != "paused":
                        status = paused["status"] if paused else "missing"
                        msg = f"Eventstream '{item.guid}' source '{source['id']}' did not pause (status: {status})"
                        raise RuntimeError(msg)
            yield
            operation_failed = False
        finally:
            errors: list[tuple[str, Exception]] = []
            for guid, source in attempted:
                try:
                    self._recover_source(guid, source)
                except Exception as error:
                    label = f"Recover Eventstream '{guid}' source '{source['id']}'"
                    errors.append((label, error))
                    logger.error(
                        "%s failed: %s. Check the source in Fabric and resume it manually if needed.", label, error
                    )
            if errors and not operation_failed:
                raise PublishError(errors, logger) from errors[0][1]

    def _stable_sources(
        self, guid: str, source_id: Optional[str] = None, *, wait_for_presence: bool = False
    ) -> list[dict]:
        """Wait for transitions without taking ownership of sources paused by someone else."""
        start_time = time.time()
        attempt = 0
        while True:
            response = self.fabric_workspace_obj.endpoint.invoke(
                method="GET", url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{guid}/topology"
            )
            sources = response["body"]["sources"]
            source_present = any(source["id"] == source_id for source in sources)
            if (not wait_for_presence or source_present) and not any(
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

    def _source_action(self, guid: str, source: dict, action: str) -> None:
        logger.info("%sEventstream '%s': %s source '%s'", constants.INDENT, guid, action, source["id"])
        kwargs = {}
        if action == "resume":
            kwargs["body"] = {"startType": "Now" if source["type"] == "SampleData" else "WhenLastStopped"}
        self.fabric_workspace_obj.endpoint.invoke(
            method="POST",
            url=f"{self.fabric_workspace_obj.base_api_url}/eventstreams/{guid}/sources/{source['id']}/{action}",
            **kwargs,
        )

    def _recover_source(self, guid: str, original_source: dict) -> None:
        source_id = original_source["id"]
        source = next((s for s in self._stable_sources(guid, source_id) if s["id"] == source_id), None)
        if source is None:
            # Topology can lag an update. Only a deployed definition confirms removal.
            response = self.fabric_workspace_obj.endpoint.invoke(
                method="POST", url=f"{self.fabric_workspace_obj.base_api_url}/items/{guid}/getDefinition"
            )
            part = next((p for p in response["body"]["definition"]["parts"] if p["path"] == "eventstream.json"), None)
            if part is None:
                msg = f"Eventstream '{guid}' deployed definition has no eventstream.json part"
                raise RuntimeError(msg)
            definition = json.loads(base64.b64decode(part["payload"], validate=True))
            if source_id not in {s["id"] for s in definition["sources"]}:
                logger.info("Eventstream '%s' source '%s' was removed/replaced; no resume needed.", guid, source_id)
                return
            source = next(
                s for s in self._stable_sources(guid, source_id, wait_for_presence=True) if s["id"] == source_id
            )
        if source["status"].lower() == "running":
            return
        if source["status"].lower() == "paused":
            if source["type"] not in _PAUSABLE_SOURCE_TYPES:
                msg = f"Eventstream '{guid}' source '{source_id}' changed to unsupported type '{source['type']}'"
                raise RuntimeError(msg)
            resume_error = None
            try:
                self._source_action(guid, source, "resume")
            except Exception as error:
                # A lost resume response is harmless only if topology confirms recovery.
                resume_error = error
            resumed = next((s for s in self._stable_sources(guid, source_id) if s["id"] == source_id), None)
            if resumed is not None and resumed["status"].lower() == "running":
                return
            if resume_error is not None:
                raise resume_error
            source = resumed
        status = source["status"] if source else "missing"
        msg = f"Eventstream '{guid}' source '{source_id}' could not be restored to Running (status: {status})"
        raise RuntimeError(msg)
