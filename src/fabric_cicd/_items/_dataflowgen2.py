# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Dataflow Gen2 item."""

import logging
import re

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import InputError, ItemDependencyError, ParsingError
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item
from fabric_cicd._parameter._utils import check_replacement, extract_find_value, extract_parameter_filters

logger = logging.getLogger(__name__)

# Constants for Dataflow Gen2 processing
DATAFLOW_DEPENDENCIES = {}
SOURCE_DATAFLOW_ID_MAPPING = {}
SOURCE_DATAFLOW_WORKSPACE_ID_MAPPING = {}


def publish_dataflows(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all dataflow items from the repository in the correct order based on their dependencies.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "Dataflow"

    # Set the publish order based on dependencies (when dataflow references another dataflow)
    publish_order = set_dataflow_publish_order(fabric_workspace_obj, item_type)

    for item_name in publish_order:
        fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=item_type, func_process_file=func_process_file
        )


def set_dataflow_publish_order(workspace_obj: FabricWorkspace, item_type: str) -> list[str]:
    """
    Sets the publish order where the source dataflow, if present always proceeds the referencing dataflow.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_type: Type of item (e.g., 'Dataflow').
    """
    publish_order = []
    visited = set()
    temp_visited = set()

    # Collect dataflow items with a source dataflow that exists in the repository
    for item in workspace_obj.repository_items.get(item_type, {}).values():
        for file in item.item_files:
            # Check if a source dataflow is referenced in the file
            if file.type == "text" and str(file.file_path).endswith(".pq") and contains_source_dataflow(file.contents):
                # Store the dataflow and its source dataflow in the constant dictionaries
                dataflow_name, dataflow_id, dataflow_workspace_id = get_source_dataflow_name(
                    workspace_obj, file.contents, item.name, file.file_path
                )
                if dataflow_name:
                    DATAFLOW_DEPENDENCIES[item.name] = dataflow_name
                    # Map the dataflow name to its ID for later use
                    SOURCE_DATAFLOW_ID_MAPPING[dataflow_name] = dataflow_id
                    SOURCE_DATAFLOW_WORKSPACE_ID_MAPPING[dataflow_name] = dataflow_workspace_id

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
            msg = f"Circular dependency found for item {item}. Cannot determine a valid publish order."
            raise ParsingError(msg, logger)

        # Add the item to the temporary visited set
        temp_visited.add(item)

        # First add the dependency if it exists
        if DATAFLOW_DEPENDENCIES.get(item):
            dependency = DATAFLOW_DEPENDENCIES[item]
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
    for item in list(DATAFLOW_DEPENDENCIES.keys()):
        add_dataflow_with_dependency(item)

    # Add any remaining dataflows from the repository that aren't in the publish order (standalone dataflows)
    for item_name in workspace_obj.repository_items.get(item_type, {}):
        if item_name not in visited:
            publish_order.append(item_name)

    return publish_order


def contains_source_dataflow(file_content: str) -> bool:
    """
    A helper function to check if the file content contains a source dataflow reference.

    Args:
        file_content: Content of the file to check.
    """
    try:
        match = re.search(constants.SOURCE_DATAFLOW_REGEX, file_content)
        return bool(match and match.group(1))
    except (re.error, TypeError, IndexError) as e:
        logger.debug(f"Error checking for source dataflow: {e}")
        return False


def get_source_dataflow_name(
    workspace_obj: FabricWorkspace, file_content: str, item_name: str, file_path: str
) -> tuple[str, str, str]:
    """
    A helper function to extract the source dataflow name associated with the dataflow ID in the
    file content using environment parameter and repository items dictionaries.

    Args:
        workspace_obj: The FabricWorkspace object.
        file_content: Content of the dataflow file.
        item_name: Name of the dataflow item.
        file_path: Path to the dataflow file.

    Returns:
        A tuple containing (dataflow_name, dataflow_id, dataflow_workspace_id)
    """
    # Check if the environment parameter 'find_replace' exists
    parameter_dict = workspace_obj.environment_parameter.get("find_replace", [])
    if not parameter_dict:
        logger.warning(
            f"'find_replace' parameter not found in the parameter dictionary. Cannot look up {item_name}'s source dataflow in the repository"
        )
        return "", "", ""

    try:
        # Extract the dataflow ID and workspace ID from the file content using regex
        match = re.search(constants.SOURCE_DATAFLOW_REGEX, file_content)
        if not match:
            msg = f"No dataflow source pattern found in file content for {file_path}"
            raise ParsingError(msg, logger)
        dataflow_id = match.group(3)
        dataflow_workspace_id = match.group(2)
    except Exception as e:
        msg = f"Error extracting dataflow information from file content: {e}"
        raise ParsingError(msg, logger) from e

    if not dataflow_id or not re.match(constants.VALID_GUID_REGEX, dataflow_id):
        msg = f"Invalid dataflow ID: {dataflow_id} in file content for {file_path}"
        raise ParsingError(msg, logger)

    if not dataflow_workspace_id or not re.match(constants.VALID_GUID_REGEX, dataflow_workspace_id):
        msg = f"Invalid workspace ID: {dataflow_workspace_id} in file content for {file_path}"
        raise ParsingError(msg, logger)

    # Create case-insensitive lookup dictionaries for repository and workspace dataflow names
    dataflow_repo_lookup = {}
    for key in workspace_obj.repository_items.get("Dataflow", {}):
        dataflow_repo_lookup[key.lower().strip()] = key

    dataflow_workspace_lookup = {}
    for key in workspace_obj.deployed_items.get("Dataflow", {}):
        dataflow_workspace_lookup[key.lower().strip()] = key

    # Look for a parameter that contains the dataflow ID
    for param in parameter_dict:
        # Extract values from the parameter
        input_type, input_name, input_path = extract_parameter_filters(workspace_obj, param)
        filter_match = check_replacement(input_type, input_name, input_path, "Dataflow", item_name, file_path)
        find_value = extract_find_value(param, file_content, filter_match)

        # Skip if this parameter doesn't match the dataflow ID
        if find_value != dataflow_id:
            logger.debug(
                f"Find value: {find_value} does not match the dataflow ID: {dataflow_id}, skipping this parameter"
            )
            continue

        # Get the replacement value for the current environment
        replace_value = param.get("replace_value", {}).get(workspace_obj.environment, "")

        # If it references a dataflow item, extract the dataflow name
        if replace_value.startswith("$items.Dataflow"):
            source_dataflow_name = replace_value.split(".")[2]
            logger.debug(
                f"Found the dataflow name {source_dataflow_name} for dataflow ID: {dataflow_id} in the replace_value"
            )

            normalized_name = source_dataflow_name.lower().strip()

            # Check if the source dataflow name exists in the repository
            if normalized_name in dataflow_repo_lookup:
                if source_dataflow_name != dataflow_repo_lookup[normalized_name]:
                    logger.debug(
                        f"Source dataflow '{source_dataflow_name}' exists in the repository as '{dataflow_repo_lookup[normalized_name]}'"
                    )
                    source_dataflow_name = dataflow_repo_lookup[normalized_name]
                return source_dataflow_name, dataflow_id, dataflow_workspace_id

            # Throw an error if the source dataflow exists in the workspace, but not in the repository
            if normalized_name in dataflow_workspace_lookup:
                msg = f"Cannot delete the source dataflow {dataflow_workspace_lookup[normalized_name]} as it is referenced by an existing dataflow: {item_name}"
                raise ItemDependencyError(msg, logger)

            msg = f"The source dataflow name '{source_dataflow_name}' was not found in the repository, please check the name is correct"
            raise InputError(msg, logger)

        logger.warning(
            f"Dataflow item '{item_name}' references a source dataflow, but dependency sorting cannot be enabled"
        )
        logger.warning(
            'To enable proper dependency sorting and ensure correct deployment order, set the replace_value to "$items.Dataflow.Source Dataflow Name.id"'
        )

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
    return replace_dataflow_id(workspace_obj, item_obj, file_obj)


def replace_dataflow_id(workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Replaces both the dataflow ID and workspace ID of the source dataflow
    with logical values for cross-environment compatibility.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        file_obj: The file object.
    """
    if str(file_obj.file_path).endswith(".pq"):
        source_dataflow = DATAFLOW_DEPENDENCIES.get(item_obj.name, "")

        # If there is a source dataflow in the dataflow item
        if source_dataflow:
            logical_id = (
                workspace_obj.repository_items.get("Dataflow", {}).get(source_dataflow, {}).logical_id
                if source_dataflow
                else None
            )
            dataflow_id = SOURCE_DATAFLOW_ID_MAPPING.get(source_dataflow)
            dataflow_workspace_id = SOURCE_DATAFLOW_WORKSPACE_ID_MAPPING.get(source_dataflow)

            # Replace the dataflow ID with its logical ID and the workspace ID with the default workspace ID
            if logical_id and dataflow_id and dataflow_workspace_id:
                file_obj.contents = file_obj.contents.replace(dataflow_id, logical_id)
                file_obj.contents = file_obj.contents.replace(dataflow_workspace_id, constants.DEFAULT_WORKSPACE_ID)
                logger.debug(
                    f"Replaced dataflow ID '{dataflow_id}' with logical ID '{logical_id}' and workspace ID '{dataflow_workspace_id}' with default workspace ID '{constants.DEFAULT_WORKSPACE_ID}' in '{item_obj.name}' file"
                )

    return file_obj.contents
