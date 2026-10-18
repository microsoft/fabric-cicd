# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy DataPipeline item."""

import logging
import re
from collections.abc import Iterator
from typing import Optional

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher, ParallelConfig
from fabric_cicd._items._manage_dependencies import set_publish_order, set_unpublish_order
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


def _reference_value(value: object) -> Optional[str]:
    """Return a literal reference or the value of an Expression wrapper."""
    if isinstance(value, dict) and value.get("type") == "Expression":
        value = value.get("value")
    return value if isinstance(value, str) else None


def _iter_pipeline_reference_ids(activities: object) -> Iterator[str]:
    """Yield referenced IDs from actual pipeline activities, including nested ones."""
    if not isinstance(activities, list):
        return

    for activity in activities:
        if not isinstance(activity, dict):
            continue
        activity_type = activity.get("type")
        properties = activity.get("typeProperties")
        if not isinstance(properties, dict):
            continue

        if activity_type == "ExecutePipeline":
            pipeline = properties.get("pipeline")
            if isinstance(pipeline, dict):
                reference_name = _reference_value(pipeline.get("referenceName"))
                if reference_name is not None:
                    yield reference_name
        elif activity_type == "InvokePipeline":
            pipeline_id = _reference_value(properties.get("pipelineId"))
            if pipeline_id is not None:
                yield pipeline_id

        # Fabric control-flow activities embed their child activity lists here.
        for field in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            yield from _iter_pipeline_reference_ids(properties.get(field))
        cases = properties.get("cases")
        if isinstance(cases, list):
            for case in cases:
                if isinstance(case, dict):
                    yield from _iter_pipeline_reference_ids(case.get("activities"))


def find_referenced_datapipelines(fabric_workspace_obj: FabricWorkspace, file_content: dict, lookup_type: str) -> list:
    """Find pipeline dependencies in ExecutePipeline and InvokePipeline activities.

    Only actual pipeline-reference fields count as dependencies. Other GUID-valued
    fields (such as pipeline parameters) must not affect publishing order.
    Nested control-flow activities are included.

    Args:
        fabric_workspace_obj: Workspace used to resolve pipeline IDs to names.
        file_content: Parsed pipeline-content.json definition.
        lookup_type: "Repository" or "Deployed" reference resolution mode.
    """
    item_type = ItemType.DATA_PIPELINE.value
    reference_list = []
    guid_pattern = re.compile(constants.VALID_GUID_REGEX)
    properties = file_content.get("properties") if isinstance(file_content, dict) else None
    activities = properties.get("activities", []) if isinstance(properties, dict) else []

    for referenced_id in _iter_pipeline_reference_ids(activities):
        if not guid_pattern.fullmatch(referenced_id):
            continue
        referenced_name = fabric_workspace_obj._convert_id_to_name(
            item_type=item_type, generic_id=referenced_id, lookup_type=lookup_type
        )
        if referenced_name and referenced_name not in reference_list:
            reference_list.append(referenced_name)

    return reference_list


def _get_datapipeline_publish_order(publisher: "DataPipelinePublisher") -> list[str]:
    """Get the ordered list of data pipeline names based on dependencies."""
    return set_publish_order(publisher.fabric_workspace_obj, publisher.item_type, find_referenced_datapipelines)


class DataPipelinePublisher(ItemPublisher):
    """Publisher for Data Pipeline items."""

    item_type = ItemType.DATA_PIPELINE.value
    has_dependency_tracking = True

    parallel_config = ParallelConfig(enabled=False, ordered_items_func=_get_datapipeline_publish_order)
    """Pipelines must be published in dependency order (sequential)"""

    def get_unpublish_order(self, items_to_unpublish: list[str]) -> list[str]:
        """
        Get the ordered list of item names based on dependencies for unpublishing.

        Args:
            items_to_unpublish: List of item names to be unpublished.

        Returns:
            List of item names in the order they should be unpublished (reverse dependency order).
        """
        return set_unpublish_order(
            self.fabric_workspace_obj, self.item_type, items_to_unpublish, find_referenced_datapipelines
        )

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Data Pipeline item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)

    def pre_publish_all(self) -> None:
        """Refresh deployed items before publishing to resolve references."""
        self.fabric_workspace_obj._refresh_deployed_items()
