# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Warehouse item."""

import json
import logging

from fabric_cicd import constants
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)


class WarehousePublisher(ItemPublisher):
    """Publisher for Warehouse items."""

    item_type = ItemType.WAREHOUSE.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Warehouse item."""
        if self.fabric_workspace_obj._apply_publish_filters(item, item_name, self.item_type):
            return

        metadata = next(
            (json.loads(file.contents)["metadata"] for file in item.item_files if file.relative_path == ".platform"),
            {},
        )
        shell_only_flag = FeatureFlag.ENABLE_WAREHOUSE_SHELL_ONLY_PUBLISH.value
        if shell_only_flag in constants.FEATURE_FLAG:
            logger.warning(
                f"Warehouse '{item_name}' is using '{shell_only_flag}': only metadata is deployed; "
                "SQL project definitions are not deployed. Creation payloads apply only to new Warehouses "
                "and cannot change the collation of existing Warehouses."
            )
            self.fabric_workspace_obj._publish_item(
                item_name=item_name,
                item_type=self.item_type,
                shell_only_publish=True,
                creation_payload=metadata.get("creationPayload"),
            )
            return

        if "creationPayload" in metadata:
            msg = (
                f"Warehouse '{item_name}' contains a legacy .platform creationPayload. "
                "Remove it to deploy a Git-exported SQL project definition, or enable "
                f"'{shell_only_flag}' to retain shell-only publishing without deploying the schema. "
                "Configure the target workspace's default collation before creating a new Warehouse; "
                "existing Warehouse collation cannot be changed."
            )
            raise InputError(msg, logger)

        sql_projects = [file for file in item.item_files if file.file_path.suffix.lower() == ".sqlproj"]
        if len(sql_projects) != 1:
            msg = (
                f"Warehouse '{item_name}' requires exactly one .sqlproj file from Fabric Git integration "
                f"for full-definition publishing; found {len(sql_projects)}. "
                f"Re-export the Warehouse through Git integration, or enable '{shell_only_flag}' "
                "to publish only its metadata."
            )
            raise InputError(msg, logger)

        if any(file.file_path.suffix.lower() == ".dacpac" for file in item.item_files):
            msg = (
                f"Warehouse '{item_name}' contains a .dacpac file. Only Git-exported sqlproj definitions "
                "are supported; remove compiled DACPAC artifacts from the item folder."
            )
            raise InputError(msg, logger)

        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            api_format=constants.API_FORMAT_MAPPING[self.item_type],
        )
