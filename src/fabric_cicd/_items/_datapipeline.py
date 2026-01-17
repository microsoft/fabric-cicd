# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy DataPipeline item."""

import logging
import re

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher, ParallelConfig
from fabric_cicd._items._manage_dependencies import set_publish_order
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


def find_referenced_datapipelines(fabric_workspace_obj: FabricWorkspace, file_content: dict, lookup_type: str) -> list:
    """
    Scan through pipeline file json dictionary and find pipeline references (including nested pipelines).

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        file_content: Dict representation of the pipeline-content file.
        lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
    """
    item_type = ItemType.DATA_PIPELINE.value
    reference_list = []
    guid_pattern = re.compile(constants.VALID_GUID_REGEX)

    # Use the dpath library to search through the dictionary for all values that match the GUID pattern
    for _, value in dpath.search(file_content, "**", yielded=True):
        if isinstance(value, str):
            match = guid_pattern.search(value)
            if match:
                # If a valid GUID is found, convert it to name. If name is not None, it's a pipeline and will be added to the reference list
                referenced_id = match.group(0)
                referenced_name = fabric_workspace_obj._convert_id_to_name(
                    item_type=item_type, generic_id=referenced_id, lookup_type=lookup_type
                )
                # Add pipeline to the reference list if it's not already present
                if referenced_name and referenced_name not in reference_list:
                    reference_list.append(referenced_name)

    return reference_list


def _get_datapipeline_publish_order(publisher: "DataPipelinePublisher") -> list[str]:
    """Get the ordered list of data pipeline names based on dependencies."""
    return set_publish_order(publisher.fabric_workspace_obj, publisher.item_type, find_referenced_datapipelines)


class DataPipelinePublisher(ItemPublisher):
    """Publisher for Data Pipeline items."""

    item_type = ItemType.DATA_PIPELINE.value

    parallel_config = ParallelConfig(enabled=False, ordered_items_func=_get_datapipeline_publish_order)
    """Pipelines must be published in dependency order (sequential)"""

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Data Pipeline item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)

    def pre_publish_all(self) -> None:
        """Refresh deployed items before publishing to resolve references."""
        self.fabric_workspace_obj._refresh_deployed_items()
