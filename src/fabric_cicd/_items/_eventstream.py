# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

import logging

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)


def _is_private_endpoint_validation_error(exc: Exception) -> bool:
    """
    Determine whether an exception corresponds to the Eventstream private-endpoint
    ``DataSourcesValidationError`` raised by the updateDefinition API.

    Args:
        exc: The exception raised while publishing the eventstream definition.
    """
    message = str(exc).lower()
    return "datasourcesvalidationerror" in message and "private endpoint" in message


def _get_eventstream_topology(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str) -> dict:
    """
    Retrieve the topology of the specified eventstream.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        eventstream_guid: The GUID of the eventstream.
    """
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/get-eventstream-topology
    response = fabric_workspace_obj.endpoint.invoke(
        method="GET",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/topology",
    )
    return response["body"]


def _get_running_sources(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str) -> list[dict]:
    """
    Return every currently running source in the eventstream topology.

    All running sources are paused before retrying the definition update. Pausing
    is a no-op for sources that do not require it, so this intentionally avoids the
    fragile logic of parsing the validation error to determine which specific
    sources are backed by a private endpoint.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        eventstream_guid: The GUID of the eventstream.
    """
    topology = _get_eventstream_topology(fabric_workspace_obj, eventstream_guid)
    sources = topology.get("sources", []) or []
    return [source for source in sources if source.get("status", "").lower() == "running"]


def _pause_source(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str, source: dict) -> None:
    """
    Pause a single eventstream source.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        eventstream_guid: The GUID of the eventstream.
        source: The source definition (requires an ``id`` and optional ``name``).
    """
    source_id = source["id"]
    source_name = source.get("name", source_id)
    logger.info(f"{constants.INDENT}Pausing Eventstream source '{source_name}'")
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/pause-eventstream-source
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/sources/{source_id}/pause",
    )


def _resume_source(fabric_workspace_obj: FabricWorkspace, eventstream_guid: str, source: dict) -> None:
    """
    Resume a single eventstream source from where it was last stopped.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        eventstream_guid: The GUID of the eventstream.
        source: The source definition (requires an ``id`` and optional ``name``).
    """
    source_id = source["id"]
    source_name = source.get("name", source_id)
    logger.info(f"{constants.INDENT}Resuming Eventstream source '{source_name}'")
    # https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/topology/resume-eventstream-source
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/eventstreams/{eventstream_guid}/sources/{source_id}/resume",
        body={"startType": "WhenLastStopped"},
    )


class EventstreamPublisher(ItemPublisher):
    """Publisher for Eventstream items."""

    item_type = ItemType.EVENTSTREAM.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """
        Publish a single Eventstream item.

        When the ``enable_eventstream_source_pause`` feature flag is not set, this behaves
        exactly like the default publisher. When the flag is set and the definition update
        fails because a source is bound to a datasource behind a private endpoint, all
        running sources are paused, the update is retried, and the sources are resumed
        afterward (always resumed via a ``finally`` block so a failure cannot leave them paused).

        Args:
            item_name: The name of the item to publish.
            item: The Item object to publish.
        """
        if FeatureFlag.ENABLE_EVENTSTREAM_SOURCE_PAUSE.value not in constants.FEATURE_FLAG:
            self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)
            return

        try:
            self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)
        except Exception as exc:
            if not _is_private_endpoint_validation_error(exc):
                raise

            # A missing GUID means the eventstream has no deployed topology yet (create path),
            # where this validation error cannot occur, so there is nothing to pause.
            eventstream_guid = item.guid
            if not eventstream_guid:
                raise

            logger.info(
                f"{constants.INDENT}Eventstream '{item_name}' update blocked by a private-endpoint data source. "
                "Pausing the affected source(s) and retrying."
            )

            paused_sources = []
            try:
                for source in _get_running_sources(self.fabric_workspace_obj, eventstream_guid):
                    _pause_source(self.fabric_workspace_obj, eventstream_guid, source)
                    paused_sources.append(source)

                self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)
            finally:
                for source in paused_sources:
                    _resume_source(self.fabric_workspace_obj, eventstream_guid, source)
