# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import base64
import json
import logging
import time

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import ItemCleanupError
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
    """Wait for source updates to finish, then return the currently running sources."""
    start_time = time.time()
    attempt = 0
    while True:
        sources = _get_topology_sources(fabric_workspace_obj, eventstream_guid)
        if not any(source.get("status", "").lower() == "updating" for source in sources):
            return [source for source in sources if source.get("status", "").lower() == "running"]
        handle_retry(
            attempt=attempt,
            base_delay=2,
            response_retry_after=20,
            prepend_message=f"{constants.INDENT}Waiting for Eventstream source update(s) to finish.",
            max_duration=constants.RETRY_MAX_DURATION_SECONDS,
            start_time=start_time,
        )
        attempt += 1


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
        # PauseFailed is returned as a valid source state by Fabric's topology action APIs
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
    """Resume Sample Data from now and other sources from where they last stopped."""
    source_name = source.get("name", source["id"])
    logger.info(f"{constants.INDENT}Resuming Eventstream source '{source_name}'")
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/resume-eventstream-source
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/sources/{source['id']}/resume",
        body={"startType": "Now" if source.get("type") == "SampleData" else "WhenLastStopped"},
    )


class EventstreamPublisher(ItemPublisher):
    """Publisher for Eventstream items."""

    item_type = ItemType.EVENTSTREAM.value
    _paused_sources: list[tuple[str, dict]]

    def setup_items(self, items: list[Item]) -> None:
        """Pause deployed sources and track attempted pauses for this operation.

        Fail fast on the first setup error to abort publishing and avoid pausing
        more sources unnecessarily. The lifecycle still runs cleanup to attempt
        recovering every attempted pause, including requests with lost responses.
        """
        self._paused_sources = []
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value not in constants.FEATURE_FLAG:
            return

        # Skip items that do not exist yet
        for item in items:
            if not item.guid:
                continue
            running_sources = _get_running_sources(self.fabric_workspace_obj, item.guid)
            for source in running_sources:
                self._paused_sources.append((item.guid, source))
                _pause_source(self.fabric_workspace_obj, item.guid, source)
            # Wait for all running sources to be paused before continuing
            if running_sources:
                _wait_for_sources_paused(
                    self.fabric_workspace_obj, item.guid, [source["id"] for source in running_sources]
                )

    def cleanup_items(self, items: list[Item]) -> None:  # noqa: ARG002
        """Reconcile attempted pauses and report every recovery failure."""
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value not in constants.FEATURE_FLAG:
            return

        paused_sources = self._paused_sources
        self._paused_sources = []
        resume_errors: list[tuple[str, Exception]] = []
        for eventstream_guid, source in paused_sources:
            try:
                topology_source = next(
                    (
                        candidate
                        for candidate in _get_topology_sources(self.fabric_workspace_obj, eventstream_guid)
                        if candidate["id"] == source["id"]
                    ),
                    None,
                )
                if topology_source is None:
                    response = self.fabric_workspace_obj.endpoint.invoke(
                        method="POST",
                        url=f"{self.fabric_workspace_obj.base_api_url}/items/{eventstream_guid}/getDefinition",
                    )
                    part = next(
                        (
                            part
                            for part in response["body"]["definition"]["parts"]
                            if part["path"] == "eventstream.json"
                        ),
                        None,
                    )
                    if part is None:
                        msg = "Deployed Eventstream definition has no eventstream.json part"
                        raise RuntimeError(msg)
                    definition = json.loads(base64.b64decode(part["payload"], validate=True))
                    defined_source_ids = {candidate["id"] for candidate in definition["sources"]}
                    if source["id"] not in defined_source_ids:
                        logger.info(
                            f"{constants.INDENT}Skipping Eventstream source '{source['id']}' in "
                            f"Eventstream '{eventstream_guid}': no longer in the deployed definition."
                        )
                        continue
                status = topology_source.get("status", "").lower() if topology_source else "missing"
                if status == "running":
                    continue
                if status == "pausing":
                    _wait_for_sources_paused(self.fabric_workspace_obj, eventstream_guid, [source["id"]])
                elif status != "paused":
                    msg = f"Cannot recover Eventstream source '{source['id']}' with topology status '{status}'"
                    raise RuntimeError(msg)
                _resume_source(self.fabric_workspace_obj, eventstream_guid, source)
            except Exception as error:
                label = f"Resume Eventstream source '{source['id']}' in Eventstream '{eventstream_guid}'"
                resume_errors.append((label, error))
        if resume_errors:
            raise ItemCleanupError(resume_errors, logger) from resume_errors[0][1]
