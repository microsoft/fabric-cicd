# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Variable Library item."""

import json
import logging
from typing import Optional

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._item import Item
from fabric_cicd._common._logging import log_header
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


def activate_value_set(
    fabric_workspace_obj: FabricWorkspace, item_obj: Item, active_value_set: Optional[str] = None
) -> None:
    """
    Activates the value set for the given Variable Library item.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        active_value_set: Explicit plan selection validated by Fabric, or None to use the workspace environment.
    """
    settings_file_obj = next((file for file in item_obj.item_files if file.name == "settings.json"), None)

    if settings_file_obj:
        settings_dict = json.loads(settings_file_obj.contents)
        if active_value_set is not None:
            if active_value_set != fabric_workspace_obj.environment:
                logger.warning(
                    f"Variable Library '{item_obj.name}' uses plan value set '{active_value_set}', "
                    f"which differs from target environment '{fabric_workspace_obj.environment}'."
                )
        elif fabric_workspace_obj.environment in settings_dict["valueSetsOrder"]:
            active_value_set = fabric_workspace_obj.environment
        else:
            active_value_set = "Default value set"
            logger.warning(
                f"Provided target environment '{fabric_workspace_obj.environment}' does not match any value sets.  Using '{active_value_set}'"
            )

        body = {"properties": {"activeValueSetName": active_value_set}}

        fabric_workspace_obj.endpoint.invoke(
            method="PATCH", url=f"{fabric_workspace_obj.base_api_url}/VariableLibraries/{item_obj.guid}", body=body
        )

        logger.info(f"{constants.INDENT}Active value set changed to '{active_value_set}'")

    else:
        if active_value_set is not None:
            msg = (
                f"Cannot activate plan value set '{active_value_set}' for '{item_obj.name}': settings.json is missing."
            )
            raise InputError(msg, logger)
        logger.warning(f"settings.json file not found for item {item_obj.name}. Active value set not changed.")


class VariableLibraryPublisher(ItemPublisher):
    """Publisher for Variable Library items."""

    item_type = ItemType.VARIABLE_LIBRARY.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Variable Library item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)
        if not item.skip_publish:
            activate_value_set(self.fabric_workspace_obj, item)

    def post_publish_all(self, value_set_selections: Optional[dict[str, Optional[str]]] = None) -> None:
        """Activate value sets after bulk upload."""
        if self.fabric_workspace_obj.bulk_publish_enabled:
            log_header(logger, "Activating Variable Library Value Sets")
            for item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).values():
                if not item.skip_publish:
                    selection = value_set_selections.get(item.name) if value_set_selections else None
                    if selection is None:
                        activate_value_set(self.fabric_workspace_obj, item)
                    else:
                        activate_value_set(self.fabric_workspace_obj, item, selection)
