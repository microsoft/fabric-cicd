# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Lakehouse item."""

import copy
import json
import logging

import dpath

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher, Publisher
from fabric_cicd.constants import FeatureFlag, ItemType

logger = logging.getLogger(__name__)


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


def _get_shortcut_path(shortcut: dict) -> str:
    """
    Build a unique shortcut path from shortcut metadata.

    Args:
        shortcut: The shortcut definition dictionary
    """
    return f"{shortcut['path']}/{shortcut['name']}"


def list_deployed_shortcuts(fabric_workspace_obj: FabricWorkspace, item_obj: Item) -> dict[str, dict]:
    """
    Lists all deployed shortcut by path

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published
        item_obj: The item object to list the shortcuts for
    """
    request_url = f"{fabric_workspace_obj.base_api_url}/items/{item_obj.guid}/shortcuts"
    deployed_shortcuts_by_path = {}

    while request_url:
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts/list-shortcuts
        response = fabric_workspace_obj.endpoint.invoke(method="GET", url=request_url)

        # Handle cases where the response body is empty
        shortcuts = response["body"].get("value", [])
        for shortcut in shortcuts:
            deployed_shortcuts_by_path[_get_shortcut_path(shortcut)] = shortcut

        request_url = response["header"].get("continuationUri", None)

    return deployed_shortcuts_by_path


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


class LakehousePublisher(ItemPublisher):
    """Publisher for Lakehouse items."""

    item_type = ItemType.LAKEHOUSE.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Lakehouse item."""
        creation_payload = next(
            (
                {"enableSchemas": True}
                for file in item.item_files
                if file.name == "lakehouse.metadata.json" and "defaultSchema" in file.contents
            ),
            None,
        )

        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            creation_payload=creation_payload,
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
            for item_obj in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).values():
                # Check if the item is published to avoid any post publish actions
                if not item_obj.skip_publish:
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

    @staticmethod
    def _is_shortcut_subset_match(repository_shortcut: object, deployed_shortcut: object) -> bool:
        """
        Recursively compare shortcut fields while ignoring extra deployed fields.

        This allows server-side schema extensions without forcing unnecessary republish.
        """
        if isinstance(repository_shortcut, dict):
            if not isinstance(deployed_shortcut, dict):
                return False
            for key, repository_value in repository_shortcut.items():
                if key not in deployed_shortcut:
                    return False
                if not ShortcutPublisher._is_shortcut_subset_match(repository_value, deployed_shortcut[key]):
                    return False
            return True

        if isinstance(repository_shortcut, list):
            if not isinstance(deployed_shortcut, list) or len(repository_shortcut) != len(deployed_shortcut):
                return False
            return all(
                ShortcutPublisher._is_shortcut_subset_match(repository_item, deployed_item)
                for repository_item, deployed_item in zip(repository_shortcut, deployed_shortcut)
            )

        return repository_shortcut == deployed_shortcut

    def _is_shortcut_changed(self, shortcut: dict, deployed_shortcut: dict) -> bool:
        """
        Determine whether a shortcut needs publishing.

        Returns:
            True when the shortcut differs from deployed state and should be published.
        """
        shortcut_for_compare = replace_default_lakehouse_id(copy.deepcopy(shortcut), self.item_obj)
        return not self._is_shortcut_subset_match(shortcut_for_compare, deployed_shortcut)

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
            (file for file in self.item_obj.item_files if file.name == "shortcuts.metadata.json"), None
        )

        if shortcut_file_obj:
            shortcut_file_obj.contents = self.fabric_workspace_obj._replace_parameters(shortcut_file_obj, self.item_obj)
            shortcut_file_obj.contents = self.fabric_workspace_obj._replace_logical_ids(shortcut_file_obj.contents)
            shortcut_file_obj.contents = self.fabric_workspace_obj._replace_workspace_ids(shortcut_file_obj.contents)

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

        shortcuts_to_publish = {_get_shortcut_path(shortcut): shortcut for shortcut in shortcuts}

        if shortcuts_to_publish:
            logger.info(f"Publishing Lakehouse '{self.item_obj.name}' Shortcuts")
            shortcut_paths_to_unpublish = [
                path for path in list(deployed_shortcuts.keys()) if path not in shortcuts_to_publish
            ]
            self._unpublish_shortcuts(shortcut_paths_to_unpublish)
            if FeatureFlag.ENABLE_SHORTCUT_SMART_DIFF.value in constants.FEATURE_FLAG:
                shortcuts_with_change = {}
                unchanged_shortcut_count = 0
                for shortcut_path, shortcut in shortcuts_to_publish.items():
                    deployed_shortcut = deployed_shortcuts.get(shortcut_path)
                    if deployed_shortcut and not self._is_shortcut_changed(shortcut, deployed_shortcut):
                        unchanged_shortcut_count += 1
                        continue
                    shortcuts_with_change[shortcut_path] = shortcut
                shortcuts_to_publish = shortcuts_with_change
                if unchanged_shortcut_count:
                    logger.info(
                        f"{constants.INDENT}Skipped {unchanged_shortcut_count} unchanged shortcut(s) via smart diff"
                    )

            # Deploy and overwrite changed/new shortcuts
            for shortcut_path, shortcut in shortcuts_to_publish.items():
                self.publish_one(shortcut_path, shortcut)
