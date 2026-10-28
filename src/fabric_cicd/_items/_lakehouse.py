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
from fabric_cicd._items._base_publisher import ItemPublisher, Publisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)

LAKEHOUSE_METADATA_FILE = "lakehouse.metadata.json"
SHORTCUTS_METADATA_FILE = "shortcuts.metadata.json"
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


def list_deployed_shortcuts(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> list:
    """
    Lists all deployed shortcut paths

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to list the shortcuts for
    """
    request_url = f"{fabric_workspace_obj.base_api_url}/items/{item_obj.guid}/shortcuts"
    deployed_shortcut_paths = []

    while request_url:
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/list-shortcuts
        response = fabric_workspace_obj.endpoint.invoke(method="GET", url=request_url)

        # Handle cases where the response body is empty
        shortcuts = response["body"].get("value", [])
        deployed_shortcut_paths.extend(f"{shortcut['path']}/{shortcut['name']}" for shortcut in shortcuts)

        request_url = response["header"].get("continuationUri", None)

    return deployed_shortcut_paths


def replace_default_lakehouse_id(shortcut: dict, item_obj: Item) -> dict:
    """
    Replaces the default lakehouse ID (all zeros) with the actual lakehouse ID
    in the shortcut definition when present.

    Args:
        shortcut: The shortcut definition dictionary
        item_obj: The item object used to get the default lakehouse ID
    """
    if dpath.get(shortcut, "target/oneLake/itemId", default=None) == constants.DEFAULT_GUID:
        shortcut["target"]["oneLake"]["itemId"] = item_obj.guid

    return shortcut


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


def func_process_file(_workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Custom file processing for Lakehouse definition files.

    Shortcuts are published separately (see ShortcutPublisher), so the Shortcuts object type in
    alm.settings.json is always set to Disabled to ensure the definition API never creates, updates,
    or deletes shortcuts. All other ALM settings (e.g., DataAccessRoles) are preserved.

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
        """Publish a single Lakehouse item with its full definition (shortcuts are published separately)."""
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
                f"{ALM_SETTINGS_FILE}; data access roles will be ignored by the Fabric API"
            )

        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            exclude_path=SHORTCUTS_EXCLUDE_PATH_REGEX,
            func_process_file=func_process_file,
            additional_parts=additional_parts,
            skip_publish_logging=True,
        )

        # Check if the item is published to avoid any post publish actions
        if item.skip_publish:
            return

        check_sqlendpoint_provision_status(self.fabric_workspace_obj, item)

        logger.info(f"{constants.INDENT}Published Lakehouse '{item_name}'")

    def post_publish_all(self) -> None:
        """Publish shortcuts after all lakehouses are published to protect interrelationships."""
        if FeatureFlag.ENABLE_SHORTCUT_PUBLISH.value in constants.FEATURE_FLAG:
            log_header(logger, "Publishing Lakehouse Shortcuts")
            for item_obj in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).values():
                # Check if the item is published to avoid any post publish actions
                if not item_obj.skip_publish and item_obj.guid:
                    shortcut_publisher = ShortcutPublisher(self.fabric_workspace_obj, item_obj)
                    shortcut_publisher.publish_all()


class ShortcutPublisher(Publisher):
    """Publisher for Lakehouse shortcuts."""

    def __init__(self, fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> None:
        """
        Initialize the shortcut publisher.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
            item_obj: The lakehouse item object to publish shortcuts for.
        """
        super().__init__(fabric_workspace_obj)
        self.item_obj = item_obj

    def _unpublish_shortcuts(self, shortcut_paths: list) -> None:
        """
        Unpublish shortcuts from the lakehouse.

        Args:
            shortcut_paths: The list of shortcut paths to unpublish.
        """
        for deployed_shortcut_path in shortcut_paths:
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/delete-shortcut
            self.fabric_workspace_obj.endpoint.invoke(
                method="DELETE",
                url=f"{self.fabric_workspace_obj.base_api_url}/items/{self.item_obj.guid}/shortcuts/{deployed_shortcut_path}",
            )

    def publish_one(self, _shortcut_name: str, shortcut: dict) -> None:
        """
        Publish a single shortcut.

        Args:
            _shortcut_name: The name/path of the shortcut to publish.
            shortcut: The shortcut definition to publish.
        """
        shortcut = replace_default_lakehouse_id(shortcut, self.item_obj)
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/create-shortcut
        try:
            self.fabric_workspace_obj.endpoint.invoke(
                method="POST",
                url=f"{self.fabric_workspace_obj.base_api_url}/items/{self.item_obj.guid}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite",
                body=shortcut,
            )
            logger.info(f"{constants.INDENT}Published Shortcut '{shortcut['name']}'")
        except Exception as e:
            if FeatureFlag.CONTINUE_ON_SHORTCUT_FAILURE.value in constants.FEATURE_FLAG:
                logger.warning(
                    f"Failed to publish Shortcut '{shortcut['name']}'. This usually happens when the lakehouse containing the source for this shortcut is published as a shell and has no data yet."
                )
                logger.info("The publish process will continue with the other items.")
                return
            msg = f"Failed to publish '{shortcut['name']}' for lakehouse {self.item_obj.name}"
            raise FailedPublishedItemStatusError(msg, logger) from e

    def publish_all(self) -> None:
        """
        Publish all shortcuts for the lakehouse item.

        Loads shortcuts from metadata, filters based on exclude regex,
        unpublishes orphaned shortcuts, and publishes all remaining shortcuts.
        """
        from fabric_cicd._common._check_utils import check_regex

        deployed_shortcuts = list_deployed_shortcuts(self.fabric_workspace_obj, self.item_obj)

        shortcut_file_obj = next(
            (file for file in self.item_obj.item_files if file.name == SHORTCUTS_METADATA_FILE), None
        )

        if shortcut_file_obj:
            shortcut_file_obj.contents = self.fabric_workspace_obj._replace_parameters(shortcut_file_obj, self.item_obj)
            shortcut_file_obj.contents = self.fabric_workspace_obj._replace_logical_ids(shortcut_file_obj.contents)
            shortcut_file_obj.contents = self.fabric_workspace_obj._replace_workspace_ids(
                shortcut_file_obj.contents, self.item_obj
            )

            shortcuts = json.loads(shortcut_file_obj.contents) or []
        else:
            logger.debug("No shortcuts.metadata.json found")
            shortcuts = []

        # Filter shortcuts based on exclude regex if provided
        if self.fabric_workspace_obj.shortcut_exclude_regex:
            regex_pattern = check_regex(self.fabric_workspace_obj.shortcut_exclude_regex)
            original_count = len(shortcuts)
            excluded_shortcuts = [s["name"] for s in shortcuts if "name" in s and regex_pattern.match(s["name"])]
            shortcuts = [s for s in shortcuts if "name" in s and not regex_pattern.match(s["name"])]
            excluded_count = original_count - len(shortcuts)
            if excluded_count > 0:
                logger.info(
                    f"{constants.INDENT}Excluded {excluded_count} shortcut(s) from {self.item_obj.name} deployment based on regex pattern"
                )
                logger.info(f"{constants.INDENT}Excluded shortcuts: {excluded_shortcuts}")

        shortcuts_to_publish = {f"{shortcut['path']}/{shortcut['name']}": shortcut for shortcut in shortcuts}

        if shortcuts_to_publish:
            logger.info(f"Publishing Lakehouse '{self.item_obj.name}' Shortcuts")
            shortcut_paths_to_unpublish = [path for path in deployed_shortcuts if path not in shortcuts_to_publish]
            self._unpublish_shortcuts(shortcut_paths_to_unpublish)
            # Deploy and overwrite shortcuts
            for shortcut_path, shortcut in shortcuts_to_publish.items():
                self.publish_one(shortcut_path, shortcut)
        else:
            logger.info(f"{constants.INDENT}No shortcuts found for Lakehouse '{self.item_obj.name}'")
