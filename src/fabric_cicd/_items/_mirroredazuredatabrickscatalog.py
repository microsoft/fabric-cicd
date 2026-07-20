# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Mirrored Azure Databricks Catalog item."""

import json
import logging
from typing import Optional

from fabric_cicd import constants
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


class MirroredAzureDatabricksCatalogPublisher(ItemPublisher):
    """Publisher for Mirrored Azure Databricks Catalog items."""

    item_type = ItemType.MIRRORED_AZURE_DATABRICKS_CATALOG.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Mirrored Azure Databricks Catalog item."""
        creation_payload = self._get_creation_payload(item)

        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            creation_payload=creation_payload,
            skip_publish_logging=True,
        )

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            return

        logger.info(f"{constants.INDENT}Published Mirrored Azure Databricks Catalog '{item_name}'")

    def _get_creation_payload(self, item: Item) -> Optional[dict]:
        """
        Extract the creationPayload from the item's .platform metadata.

        The Mirrored Azure Databricks Catalog is created via the creationPayload
        (catalogName, mirroringMode, databricksWorkspaceConnectionId, storageConnectionId).
        Environment-specific values in the creationPayload (e.g., connection IDs) are
        resolved via parameterization before extraction.

        Args:
            item: The item object to extract the creationPayload from.
        """
        platform_file = next((file for file in item.item_files if file.name == ".platform"), None)
        if platform_file is None or "creationPayload" not in platform_file.contents:
            return None

        # Resolve environment-specific values (e.g., connection IDs) via parameterization
        contents = self.fabric_workspace_obj._replace_parameters(platform_file, item)

        return json.loads(contents)["metadata"]["creationPayload"]
