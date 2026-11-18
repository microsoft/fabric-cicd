# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Lakehouse item."""

import base64
import json
import logging
from typing import Optional

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, ParsingError
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item
from fabric_cicd._common._logging import log_header
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd._items._shortcuts import ShortcutPublisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)

LAKEHOUSE_METADATA_FILE = "lakehouse.metadata.json"
DATA_ACCESS_ROLES_FILE = "data-access-roles.json"
ALM_SETTINGS_FILE = "alm.settings.json"
ALM_SETTINGS_VERSION = "1.0.1"
ALM_SHORTCUTS_OBJECT_TYPE = "Shortcuts"
ALM_DATA_ACCESS_ROLES_OBJECT_TYPE = "DataAccessRoles"
ALM_STATE_ENABLED = "Enabled"
ALM_STATE_DISABLED = "Disabled"
# Shortcuts are published separately via ShortcutPublisher, so they are excluded from the item definition
SHORTCUTS_EXCLUDE_PATH_REGEX = r"^shortcuts\.metadata\.json$"


def check_sqlendpoint_provision_status(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> None:
    """
    Check the SQL endpoint status of the published lakehouses

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to check the SQL endpoint status for

    """
    iteration = 1

    while True:
        sql_endpoint_status = None

        response_state = fabric_workspace_obj.endpoint.invoke(
            method="GET", url=f"{fabric_workspace_obj.base_api_url}/lakehouses/{item_obj.guid}"
        )

        sql_endpoint_status = dpath.get(
            response_state, "body/properties/sqlEndpointProperties/provisioningStatus", default=None
        )

        if sql_endpoint_status == "Success":
            logger.info(f"{constants.INDENT}SQL Endpoint provisioned successfully")
            break

        if sql_endpoint_status == "Failed":
            msg = f"Cannot resolve SQL endpoint for lakehouse {item_obj.name}"
            raise FailedPublishedItemStatusError(msg, logger)

        handle_retry(
            attempt=iteration,
            base_delay=5,
            response_retry_after=30,
            prepend_message=f"{constants.INDENT}SQL Endpoint provisioning in progress",
        )
        iteration += 1


def _set_shortcuts_disabled(alm_settings: dict) -> dict:
    """
    Sets the Shortcuts object type (and all of its sub-object types) to Disabled in the ALM settings,
    adding the Shortcuts object type if it is not present. All other object types are preserved.

    Args:
        alm_settings: The parsed alm.settings.json dictionary
    """
    object_types = alm_settings.get("objectTypes")
    if not isinstance(object_types, list):
        object_types = []
        alm_settings["objectTypes"] = object_types

    shortcuts_type = next(
        (obj for obj in object_types if isinstance(obj, dict) and obj.get("name") == ALM_SHORTCUTS_OBJECT_TYPE), None
    )
    if shortcuts_type is None:
        shortcuts_type = {"name": ALM_SHORTCUTS_OBJECT_TYPE}
        object_types.append(shortcuts_type)

    shortcuts_type["state"] = ALM_STATE_DISABLED
    for sub_object_type in shortcuts_type.get("subObjectTypes") or []:
        if isinstance(sub_object_type, dict):
            sub_object_type["state"] = ALM_STATE_DISABLED

    return alm_settings


def is_data_access_roles_enabled(alm_settings_contents: Optional[str]) -> bool:
    """
    Returns True if the DataAccessRoles object type is Enabled in the given alm.settings.json contents.
    DataAccessRoles are disabled by default when ALM settings are absent.

    Args:
        alm_settings_contents: The raw alm.settings.json contents, or None if the file is absent
    """
    if not alm_settings_contents:
        return False
    try:
        alm_settings = json.loads(alm_settings_contents)
    except json.JSONDecodeError:
        return False
    object_types = alm_settings.get("objectTypes") if isinstance(alm_settings, dict) else None
    return any(
        isinstance(obj, dict)
        and obj.get("name") == ALM_DATA_ACCESS_ROLES_OBJECT_TYPE
        and str(obj.get("state", "")).lower() == ALM_STATE_ENABLED.lower()
        for obj in object_types or []
    )


def func_finalize_file(_workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Finalize Lakehouse definition files after all replacements.

    Shortcuts are published separately (see ShortcutPublisher), so the Shortcuts object type in
    alm.settings.json is set to Disabled after parameter replacement to ensure the definition API
    never creates, updates, or deletes shortcuts. Other parameterized ALM settings are preserved.

    Args:
        _workspace_obj: The FabricWorkspace object
        item_obj: The item object
        file_obj: The file object being processed
    """
    if file_obj.relative_path != ALM_SETTINGS_FILE:
        return file_obj.contents

    try:
        alm_settings = json.loads(file_obj.contents)
    except json.JSONDecodeError as e:
        msg = f"Invalid JSON in '{ALM_SETTINGS_FILE}' for Lakehouse '{item_obj.name}': {e}"
        raise ParsingError(msg, logger) from e

    if not isinstance(alm_settings, dict):
        msg = f"Invalid '{ALM_SETTINGS_FILE}' for Lakehouse '{item_obj.name}': expected a JSON object"
        raise ParsingError(msg, logger)

    logger.debug(f"Disabling Shortcuts in {ALM_SETTINGS_FILE} for Lakehouse '{item_obj.name}'")
    return json.dumps(_set_shortcuts_disabled(alm_settings), indent=2)


def _build_part(path: str, contents: str) -> dict:
    """Build an InlineBase64 definition part from text contents."""
    return {
        "path": path,
        "payload": base64.b64encode(contents.encode("utf-8")).decode("utf-8"),
        "payloadType": "InlineBase64",
    }


class LakehousePublisher(ItemPublisher):
    """Publisher for Lakehouse items."""

    item_type = ItemType.LAKEHOUSE.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a Lakehouse shell, or its full definition when explicitly enabled."""
        if FeatureFlag.ENABLE_LAKEHOUSE_DEFINITION_PUBLISH.value in constants.FEATURE_FLAG:
            self._publish_definition(item_name, item)
        else:
            creation_payload = next(
                (
                    {"enableSchemas": True}
                    for file in item.item_files
                    if file.relative_path == LAKEHOUSE_METADATA_FILE and "defaultSchema" in file.contents
                ),
                None,
            )
            self.fabric_workspace_obj._publish_item(
                item_name=item_name,
                item_type=self.item_type,
                shell_only_publish=True,
                creation_payload=creation_payload,
                skip_publish_logging=True,
            )

        if item.skip_publish:
            return

        check_sqlendpoint_provision_status(self.fabric_workspace_obj, item)

        logger.info(f"{constants.INDENT}Published Lakehouse '{item_name}'")

    def _publish_definition(self, item_name: str, item: Item) -> None:
        """Publish the full Lakehouse definition, excluding separately managed shortcuts."""
        files_by_path = {file.relative_path: file for file in item.item_files}
        additional_parts = []

        if LAKEHOUSE_METADATA_FILE not in files_by_path:
            # lakehouse.metadata.json is a required definition part
            logger.debug(f"No {LAKEHOUSE_METADATA_FILE} found for Lakehouse '{item_name}', using default '{{}}'")
            additional_parts.append(_build_part(LAKEHOUSE_METADATA_FILE, "{}"))

        alm_settings_file = files_by_path.get(ALM_SETTINGS_FILE)
        if alm_settings_file is None:
            # Shortcuts are enabled by default when ALM settings are absent, so explicitly disable them
            additional_parts.append(
                _build_part(
                    ALM_SETTINGS_FILE,
                    json.dumps(_set_shortcuts_disabled({"version": ALM_SETTINGS_VERSION, "objectTypes": []}), indent=2),
                )
            )

        if DATA_ACCESS_ROLES_FILE in files_by_path and not is_data_access_roles_enabled(
            alm_settings_file.contents if alm_settings_file else None
        ):
            logger.debug(
                f"{DATA_ACCESS_ROLES_FILE} found for Lakehouse '{item_name}' but DataAccessRoles is not enabled in "
                f"the source {ALM_SETTINGS_FILE}; roles require it to be enabled in the final parameterized settings"
            )

        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            shell_only_publish=False,
            exclude_path=SHORTCUTS_EXCLUDE_PATH_REGEX,
            func_finalize_file=func_finalize_file,
            additional_parts=additional_parts,
            skip_publish_logging=True,
        )

    def post_publish_all(self) -> None:
        """Publish shortcuts after all lakehouses are published to protect interrelationships."""
        if FeatureFlag.ENABLE_SHORTCUT_PUBLISH.value in constants.FEATURE_FLAG:
            log_header(logger, "Publishing Lakehouse Shortcuts")
            for item_obj in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).values():
                # Check if the item is published to avoid any post publish actions
                if not item_obj.skip_publish and item_obj.guid:
                    shortcut_publisher = ShortcutPublisher(self.fabric_workspace_obj, item_obj)
                    shortcut_publisher.publish_all()
