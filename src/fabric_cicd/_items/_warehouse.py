# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Warehouse item."""

import json
import logging

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)


class WarehousePublisher(ItemPublisher):
    """Publisher for Warehouse items."""

    item_type = ItemType.WAREHOUSE.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Warehouse item."""
        creation_payload = next(
            (
                json.loads(file.contents)["metadata"].get("creationPayload")
                for file in item.item_files
                if file.relative_path == ".platform"
            ),
            None,
        )
        # Publish Warehouse item as shell-only when definition publishing is not enabled, creation payload exists, or no SQL project is present
        if (
            FeatureFlag.ENABLE_WAREHOUSE_DEFINITION_PUBLISH.value not in constants.FEATURE_FLAG
            or creation_payload is not None
            or not any(file.file_path.suffix.lower() == ".sqlproj" for file in item.item_files)
        ):
            log = (
                logger.warning
                if FeatureFlag.ENABLE_WAREHOUSE_DEFINITION_PUBLISH.value in constants.FEATURE_FLAG
                else logger.debug
            )
            log(f"Warehouse '{item_name}': shell-only publishing; schema is not deployed.")
            self.fabric_workspace_obj._publish_item(
                item_name=item_name,
                item_type=self.item_type,
                shell_only_publish=True,
                creation_payload=creation_payload,
            )
            return
        # Otherwise, publish Warehouse item with full SQL project definition
        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            api_format=constants.API_FORMAT_MAPPING[self.item_type],
        )
