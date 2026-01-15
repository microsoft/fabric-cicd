# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Dataflow Gen2 item."""

import logging
import re

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import ParsingError
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd._parameter._utils import (
    check_replacement,
    extract_find_value,
    extract_parameter_filters,
    extract_replace_value,
)
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


def set_dataflow_publish_order(workspace_obj: FabricWorkspace, item_type: str) -> list[str]:
    """
    Sets the publish order where the source dataflow, if present always proceeds the referencing dataflow.
    This only applies to dataflows that reference another dataflow in the repository and the referenced
    dataflowId is parameterized correctly (using the items attribute variable).

    Algorithm for determining dataflow publish order:
    1. Find all dataflows that reference other dataflows in the repository
    2. Build a dependency graph where each dataflow depends on its source dataflow
    3. Use a modified depth-first search with cycle detection to create a topological sort
       ensuring that source dataflows are published before the dataflows that reference them
    4. Add any remaining standalone dataflows (without dependencies) to the end of the publish order

    Args:
        workspace_obj: The FabricWorkspace object.
        item_type: Type of item (e.g., 'Dataflow').
    """
    publish_order = []
    visited = set()
    temp_visited = set()

    # If the find_replace parameter doesn't exist, skip sorting dataflows by dependencies
    param_dict = workspace_obj.environment_parameter.get("find_replace", [])
    if not param_dict:
        logger.warning(
            "find_replace parameter not found - dataflows will not be checked for dependencies. "
            "Dataflows will be published in arbitrary order, which may cause errors in the deployed items"
        )
        return list(workspace_obj.repository_items.get(item_type, {}).keys())

    # Otherwise, collect dataflow items with a source dataflow
    for item in workspace_obj.repository_items.get(item_type, {}).values():
        for file in item.item_files:
            # Check if a dataflow is referenced in the file
            if file.type == "text" and str(file.file_path).endswith(".pq") and contains_source_dataflow(file.contents):
                # Try to get info associated with the dataflow reference
                dataflow_name, dataflow_workspace_id, dataflow_id = get_source_dataflow_name(
                    workspace_obj, file.contents, item.name, file.file_path
                )
                # If the dataflow is found in the repository, add it to the dependencies dictionary
                if dataflow_name:
                    workspace_obj.dataflow_dependencies[item.name] = {
                        "source_name": dataflow_name,
                        "source_workspace_id": dataflow_workspace_id,
                        "source_id": dataflow_id,
                    }
                else:
                    logger.warning(f"The '{item.name}' dataflow will be published without considering its dependency")

    def add_dataflow_with_dependency(item: str) -> bool:
        """
        Recursively adds an item and its dependency to the publish order.
        Returns True if successful, False if a cycle is detected.
        """
        # Dataflow was already processed, no need to process again
        if item in visited:
            return True

        # If the item is already in the temporary visited set, it indicates a cycle
        if item in temp_visited:
            msg = f"Circular dependency found for item {item}. Cannot determine a valid publish order"
            raise ParsingError(msg, logger)

        # Add the item to the temporary visited set
        temp_visited.add(item)

        # First add the dependency if it exists
        if workspace_obj.dataflow_dependencies.get(item):
            dependency = workspace_obj.dataflow_dependencies[item]["source_name"]
            # Propagate cycle detection
            if not add_dataflow_with_dependency(dependency):
                return False

        # Then add the current item
        publish_order.append(item)
        visited.add(item)
        # Remove from temporary set
        temp_visited.remove(item)

        return True

    # Process each item in the dataflow dependencies
    for item in list(workspace_obj.dataflow_dependencies.keys()):
        add_dataflow_with_dependency(item)

    # Add any remaining dataflows from the repository that aren't in the publish order (standalone dataflows)
    for item_name in workspace_obj.repository_items.get(item_type, {}):
        if item_name not in visited:
            publish_order.append(item_name)

    return publish_order


def contains_source_dataflow(file_content: str) -> bool:
    """A helper function to check if the file content contains a source dataflow reference."""
    try:
        # Check if file contains the PowerPlatform.Dataflows pattern (group 1 of the regex)
        match = re.search(constants.DATAFLOW_SOURCE_REGEX, file_content, re.DOTALL)
        return match is not None and bool(match.group(1))
    except (re.error, TypeError, IndexError) as e:
        logger.debug(f"Error checking for source dataflow: {e}")
        return False


def get_source_dataflow_ids(file_content: str, item_name: str) -> tuple[str, str]:
    """A helper function to get the dataflow ID and workspace ID of a referenced dataflow."""
    try:
        match = re.search(constants.DATAFLOW_SOURCE_REGEX, file_content, re.DOTALL)
        if not match:
            msg = f"No dataflow source pattern found in the '{item_name}' file content"
            raise ParsingError(msg, logger)

        # Extract the source dataflow IDs from the regex match
        dataflow_workspace_id = match.group(2)
        dataflow_id = match.group(3)

    except Exception as e:
        msg = f"Error extracting dataflow information from file content: {e}"
        raise ParsingError(msg, logger) from e

    # Validate the extracted IDs are valid GUIDs
    if not dataflow_workspace_id or not re.match(constants.VALID_GUID_REGEX, dataflow_workspace_id):
        msg = f"Invalid workspace ID: {dataflow_workspace_id} in '{item_name}' file content"
        raise ParsingError(msg, logger)
    if not dataflow_id or not re.match(constants.VALID_GUID_REGEX, dataflow_id):
        msg = f"Invalid dataflow ID: {dataflow_id} in '{item_name}' file content"
        raise ParsingError(msg, logger)

    return dataflow_workspace_id, dataflow_id


def get_source_dataflow_name(
    workspace_obj: FabricWorkspace, file_content: str, item_name: str, file_path: str
) -> tuple[str, str, str]:
    """
    A helper function to extract the dataflow name, dataflow workspaceId and dataflowId
    associated with the source dataflow referenced in the file content. The source dataflow
    name is obtained by using the matching parameter dictionary input, if present.
    """
    # Get the IDs of the source dataflow
    dataflow_workspace_id, dataflow_id = get_source_dataflow_ids(file_content, item_name)

    # Look for a parameter that contains the dataflow ID
    for param in workspace_obj.environment_parameter.get("find_replace", []):
        # Extract values from the parameter
        input_type, input_name, input_path = extract_parameter_filters(workspace_obj, param)
        filter_match = check_replacement(input_type, input_name, input_path, "Dataflow", item_name, file_path)
        find_info = extract_find_value(param, file_content, filter_match)

        # Skip if this parameter doesn't match the dataflow ID
        if find_info["pattern"] != dataflow_id:
            logger.debug(
                f"Find value: {find_info['pattern']} does not match the dataflow ID: {dataflow_id}, skipping this parameter"
            )
            continue

        # Extract the replace value for the current environment
        replace_value = param.get("replace_value", {}).get(workspace_obj.environment, "")

        # Pass in get_dataflow_name=True to get the source dataflow name, if it exists
        source_dataflow_name = extract_replace_value(workspace_obj, replace_value, get_dataflow_name=True)

        if source_dataflow_name:
            # Return the source dataflow name along with the IDs
            return source_dataflow_name, dataflow_workspace_id, dataflow_id

    return "", "", ""


def func_process_file(workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Custom file processing for dataflow items.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        file_obj: The file object.
    """
    # Replace the dataflow ID with the logical ID of the source dataflow in the file content
    return replace_source_dataflow_ids(workspace_obj, item_obj, file_obj)


def replace_source_dataflow_ids(workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Replaces both the dataflow ID and workspace ID of the source dataflow
    with logical values for cross-environment compatibility.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        file_obj: The file object.
    """
    if str(file_obj.file_path).endswith(".pq"):
        # Get source dataflow info from the dependency dictionary
        source_dataflow_info = workspace_obj.dataflow_dependencies.get(item_obj.name, {})

        # If the info was tracked, proceed to replace the IDs
        if source_dataflow_info:
            source_dataflow_name = source_dataflow_info["source_name"]
            source_dataflow_workspace_id = source_dataflow_info["source_workspace_id"]
            source_dataflow_id = source_dataflow_info["source_id"]

            # Get the logical ID of the source dataflow from repository items
            logical_id = workspace_obj.repository_items.get("Dataflow", {}).get(source_dataflow_name, {}).logical_id

            # Replace the dataflow ID with its logical ID and the workspace ID with the default workspace ID
            if logical_id:
                file_obj.contents = file_obj.contents.replace(source_dataflow_id, logical_id)
                file_obj.contents = file_obj.contents.replace(source_dataflow_workspace_id, constants.DEFAULT_GUID)
                logger.debug(
                    f"Replaced dataflow ID '{source_dataflow_id}' with logical ID '{logical_id}' and workspace ID "
                    f"'{source_dataflow_workspace_id}' with default workspace ID '{constants.DEFAULT_GUID}' "
                    f"in '{item_obj.name}' file"
                )

    return file_obj.contents


class DataflowPublisher(ItemPublisher):
    """Publisher for Dataflow items."""

    item_type = ItemType.DATAFLOW.value

    def publish_one(self, item_name: str, _item: Item) -> None:
        """Publish a single Dataflow item."""
        self.fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=self.item_type, func_process_file=func_process_file
        )

    def publish_all(self) -> None:
        """Publish all Dataflow items."""
        publish_order = set_dataflow_publish_order(self.fabric_workspace_obj, self.item_type)
        items_dict = self.fabric_workspace_obj.repository_items.get(self.item_type, {})

        for item_name in publish_order:
            item = items_dict[item_name]
            self.publish_one(item_name, item)
