# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the FabricWorkspace class to manage and publish workspace items to the Fabric API."""

import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

import dpath
from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_regex, check_valid_json_content
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InputError, ParameterFileError, ParsingError
from fabric_cicd._common._fabric_endpoint import FabricEndpoint
from fabric_cicd._common._item import Item
from fabric_cicd._common._logging import print_header

logger = logging.getLogger(__name__)


class FabricWorkspace:
    """A class to manage and publish workspace items to the Fabric API."""

    def __init__(
        self,
        repository_directory: str,
        item_type_in_scope: Optional[list[str]] = None,
        environment: str = "N/A",
        workspace_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
        token_credential: TokenCredential = None,
        **kwargs,
    ) -> None:
        """
        Initializes the FabricWorkspace instance.

        Args:
            workspace_id: The ID of the workspace to interact with. Either `workspace_id` or `workspace_name` must be provided. Considers only `workspace_id` if both are specified.
            workspace_name: The name of the workspace to interact with. Either `workspace_id` or `workspace_name` must be provided. Considers only `workspace_id` if both are specified.
            repository_directory: Local directory path of the repository where items are to be deployed from.
            item_type_in_scope: Item types that should be deployed for a given workspace. If omitted, defaults to all available item types.
            environment: The environment to be used for parameterization.
            token_credential: The token credential to use for API requests.
            kwargs: Additional keyword arguments.

        Examples:
            Basic usage
            >>> from fabric_cicd import FabricWorkspace
            >>> workspace = FabricWorkspace(
            ...     workspace_id="your-workspace-id",
            ...     repository_directory="/path/to/repo",
            ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
            ... )

            Basic usage with workspace_name
            >>> from fabric_cicd import FabricWorkspace
            >>> workspace = FabricWorkspace(
            ...     workspace_name="your-workspace-name",
            ...     repository_directory="/path/to/repo"
            ... )

            With optional parameters
            >>> from fabric_cicd import FabricWorkspace
            >>> workspace = FabricWorkspace(
            ...     workspace_id="your-workspace-id",
            ...     repository_directory="/your/path/to/repo",
            ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"],
            ...     environment="your-target-environment"
            ... )

            With token credential
            >>> from fabric_cicd import FabricWorkspace
            >>> from azure.identity import ClientSecretCredential
            >>> client_id = "your-client-id"
            >>> client_secret = "your-client-secret"
            >>> tenant_id = "your-tenant-id"
            >>> token_credential = ClientSecretCredential(
            ...     client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
            ... )
            >>> workspace = FabricWorkspace(
            ...     workspace_id="your-workspace-id",
            ...     repository_directory="/your/path/to/repo",
            ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"],
            ...     token_credential=token_credential
            ... )
        """
        from fabric_cicd._common._validate_input import (
            validate_environment,
            validate_item_type_in_scope,
            validate_repository_directory,
            validate_token_credential,
            validate_workspace_id,
            validate_workspace_name,
        )

        # Initialize endpoint
        self.endpoint = FabricEndpoint(
            # if credential is not defined, use DefaultAzureCredential
            token_credential=(
                # CodeQL [SM05139] Public library needing to have a default auth when user doesn't provide token. Not internal Azure product.
                DefaultAzureCredential() if token_credential is None else validate_token_credential(token_credential)
            )
        )

        # Set workspace_id class variable
        if workspace_id:
            self.workspace_id = validate_workspace_id(workspace_id)
        elif workspace_name:
            self.workspace_id = self._resolve_workspace_id(validate_workspace_name(workspace_name))
        else:
            msg = "Either workspace_name or workspace_id must be specified."
            raise InputError(msg, logger)

        # Validate and set class variables
        self.repository_directory: Path = validate_repository_directory(repository_directory)

        # Handle None case for item_type_in_scope by defaulting to all available item types
        if item_type_in_scope is None:
            self.item_type_in_scope = list(constants.ACCEPTED_ITEM_TYPES)
        else:
            self.item_type_in_scope = validate_item_type_in_scope(item_type_in_scope)
        self.environment = validate_environment(environment)
        self.publish_item_name_exclude_regex = None
        self.publish_folder_path_exclude_regex = None
        self.items_to_include = None
        self.responses = None
        self.repository_folders = {}
        self.repository_items = {}
        self.deployed_folders = {}
        self.deployed_items = {}

        # Initialize dataflow dependencies dictionary (used in dataflow item processing)
        self.dataflow_dependencies = {}

        # Get parameter_file_path from kwargs
        self.parameter_file_path = kwargs.get("parameter_file_path")

        # base_api_url is no longer supported - raise error if provided
        if "base_api_url" in kwargs:
            msg = (
                "Setting base_api_url is no longer supported. Please use the following instead:\n"
                ">>> import fabric_cicd.constants\n"
                ">>> constants.DEFAULT_API_ROOT_URL = '<your_base_api_url>'"
            )
            raise InputError(msg, logger)

        # Initialize dictionaries to store repository and deployed items
        self._refresh_parameter_file()

    @property
    def base_api_url(self) -> str:
        """Construct the base API URL using constants."""
        return f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{self.workspace_id}"

    def _resolve_workspace_id(self, workspace_name: str) -> str:
        """Resolve workspace ID based on the workspace name given."""
        response = self.endpoint.invoke(method="GET", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces")
        for workspace in response["body"]["value"]:
            if workspace["displayName"] == workspace_name:
                return workspace["id"]
        msg = f"Workspace ID could not be resolved from workspace name: {workspace_name}."
        raise InputError(msg, logger)

    def _lookup_item_attribute(self, workspace_id: str, item_type: str, item_name: str, attribute_name: str) -> str:
        """Lookup item attribute in the specified workspace based on item type and name."""
        response = self.endpoint.invoke(
            method="GET", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}/items"
        )
        for item in response["body"]["value"]:
            if item["type"] == item_type and item["displayName"] == item_name:
                item_guid = item["id"]
                if attribute_name == "id":
                    return item_guid
                # For other attribute, use the item guid to get the attribute value
                return self._get_item_attribute(workspace_id, item_type, item_guid, item_name, attribute_name)

        msg = f"Failed to look up item in workspace: {workspace_id}, item_type: {item_type}, item_name: {item_name}"
        raise InputError(msg, logger)

    def _get_item_attribute(
        self, workspace_id: str, item_type: str, item_guid: str, item_name: str, attribute_name: str
    ) -> str:
        """Returns the attribute value of an item in the specified workspace based on item type and id"""
        # No need to make API calls if we don't have an item guid
        if not item_guid:
            return ""

        # Check if this item type has property mappings
        if item_type not in constants.PROPERTY_PATH_ATTR_MAPPING:
            logger.debug(f"No property path mappings defined for {item_type}")
            return ""

        # Get the attribute mappings for this item type
        attribute_mappings = constants.PROPERTY_PATH_ATTR_MAPPING.get(item_type)

        # Check if the requested attribute is supported
        if attribute_name not in attribute_mappings:
            logger.debug(
                f"Attribute '{attribute_name}' not supported for {item_type} '{item_name}'. Supported: {list(attribute_mappings.keys())}"
            )
            return ""

        # Get the property path for this attribute
        property_path = attribute_mappings[attribute_name]

        response = self.endpoint.invoke(
            method="GET",
            url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}/{item_type.lower()}s/{item_guid}",
        )
        # Extract the attribute value using the path
        attribute_value = dpath.get(response, property_path, default="")
        if not attribute_value:
            msg = f"Attribute value not found for {item_type} '{item_name}'"
            raise InputError(msg, logger)

        return attribute_value

    def _refresh_parameter_file(self) -> None:
        """Load parameters if file is present."""
        from fabric_cicd._parameter._parameter import Parameter

        print_header("Validating Parameter File")

        # Initialize the parameter dict and Parameter object
        self.environment_parameter = {}
        parameter_obj = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
            parameter_file_name=constants.PARAMETER_FILE_NAME,
            parameter_file_path=self.parameter_file_path,
        )
        is_valid = parameter_obj._validate_parameter_file()
        if is_valid:
            self.environment_parameter = parameter_obj.environment_parameter
        else:
            msg = "Deployment terminated due to an invalid parameter file"
            raise ParameterFileError(msg, logger)

    def _refresh_repository_items(self) -> None:
        """Refreshes the repository_items dictionary by scanning the repository directory."""
        self.repository_items = {}
        empty_logical_id_paths = []  # Collect all paths with empty logical IDs
        visited_logical_ids = set()  # Track visited logical IDs to avoid duplicates

        for root, _dirs, files in os.walk(self.repository_directory):
            directory = Path(root)
            # valid item directory with .platform file within
            if ".platform" in files:
                item_metadata_path = directory / ".platform"

                # Print a warning and skip directory if empty
                if not any(directory.iterdir()):
                    logger.warning(f"Directory {directory.name} is empty.")
                    continue

                # Attempt to read metadata file
                try:
                    with Path.open(item_metadata_path, encoding="utf-8") as file:
                        item_metadata = json.load(file)
                except FileNotFoundError as e:
                    msg = f"{item_metadata_path} path does not exist in the specified repository. {e}"
                    ParsingError(msg, logger)
                except json.JSONDecodeError as e:
                    msg = f"Error decoding JSON in {item_metadata_path}. {e}"
                    ParsingError(msg, logger)

                # Ensure required metadata fields are present
                if "type" not in item_metadata["metadata"] or "displayName" not in item_metadata["metadata"]:
                    msg = f"displayName & type are required in {item_metadata_path}"
                    raise ParsingError(msg, logger)

                item_type = item_metadata["metadata"]["type"]
                item_description = item_metadata["metadata"].get("description", "")
                item_name = item_metadata["metadata"]["displayName"]
                item_logical_id = item_metadata["config"]["logicalId"]

                # Check for empty logical ID and collect the path
                if not item_logical_id or item_logical_id.strip() == "":
                    empty_logical_id_paths.append(str(item_metadata_path))
                    continue  # Skip processing this item further

                if item_logical_id not in visited_logical_ids:
                    visited_logical_ids.add(item_logical_id)
                else:
                    msg = f"Duplicate logicalId '{item_logical_id}' found in {item_metadata_path}"
                    raise FailedPublishedItemStatusError(msg, logger)

                item_path = directory
                relative_path = f"/{directory.relative_to(self.repository_directory).as_posix()}"
                relative_parent_path = "/".join(relative_path.split("/")[:-1])
                if "disable_workspace_folder_publish" not in constants.FEATURE_FLAG:
                    item_folder_id = self.repository_folders.get(relative_parent_path, "")
                else:
                    item_folder_id = ""

                # Get the GUID if the item is already deployed
                item_guid = self.deployed_items.get(item_type, {}).get(item_name, Item("", "", "", "")).guid

                if item_type not in self.repository_items:
                    self.repository_items[item_type] = {}

                # Add the item to the repository_items dictionary
                self.repository_items[item_type][item_name] = Item(
                    type=item_type,
                    name=item_name,
                    description=item_description,
                    guid=item_guid,
                    logical_id=item_logical_id,
                    path=item_path,
                    folder_id=item_folder_id,
                )

                self.repository_items[item_type][item_name].collect_item_files()

        # If we found any empty logical IDs, raise an error with all paths
        if empty_logical_id_paths:
            if len(empty_logical_id_paths) == 1:
                msg = f"logicalId cannot be empty in {empty_logical_id_paths[0]}"
            else:
                paths_list = "\n  - ".join(empty_logical_id_paths)
                msg = f"logicalId cannot be empty in the following files:\n  - {paths_list}"
            raise ParsingError(msg, logger)

    def _refresh_deployed_items(self) -> None:
        """Refreshes the deployed_items dictionary by querying the Fabric workspace items API."""
        # Get all items in workspace
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item
        response = self.endpoint.invoke(method="GET", url=f"{self.base_api_url}/items")

        self.deployed_items = {}
        self.workspace_items = {}

        for item in response["body"]["value"]:
            item_type = item["type"]
            item_description = item["description"]
            item_name = item["displayName"]
            item_guid = item["id"]
            item_folder_id = item.get("folderId", "")
            sql_endpoint = ""
            sql_endpoint_id = ""
            query_service_uri = ""

            # Add an empty dictionary if the item type hasn't been added yet
            if item_type not in self.deployed_items:
                self.deployed_items[item_type] = {}

            if item_type not in self.workspace_items:
                self.workspace_items[item_type] = {}

            # Get additional properties
            if item_type in ["Lakehouse", "Warehouse"]:
                sql_endpoint = self._get_item_attribute(
                    self.workspace_id, item_type, item_guid, item_name, "sqlendpoint"
                )
                sql_endpoint_id = self._get_item_attribute(
                    self.workspace_id, item_type, item_guid, item_name, "sqlendpointid"
                )
            if item_type in ["Eventhouse"]:
                query_service_uri = self._get_item_attribute(
                    self.workspace_id, item_type, item_guid, item_name, "queryserviceuri"
                )

            # Add item details to the deployed_items dictionary
            self.deployed_items[item_type][item_name] = Item(
                type=item_type,
                name=item_name,
                description=item_description,
                guid=item_guid,
                folder_id=item_folder_id,
            )

            # Add item details to the workspace_items dictionary required for parameterization (public-facing attributes)
            self.workspace_items[item_type][item_name] = {
                "id": item_guid,
                "sqlendpoint": sql_endpoint,
                "sqlendpointid": sql_endpoint_id,
                "queryserviceuri": query_service_uri,
            }

    def _replace_logical_ids(self, raw_file: str) -> str:
        """
        Replaces logical IDs with deployed GUIDs in the raw file content.

        Args:
            raw_file: The raw file content where logical IDs need to be replaced.
        """
        for item_name in self.repository_items.values():
            for item_details in item_name.values():
                logical_id = item_details.logical_id
                item_guid = item_details.guid

                if logical_id in raw_file:
                    if item_guid == "":
                        msg = f"Cannot replace logical ID '{logical_id}' as referenced item is not yet deployed."
                        raise ParsingError(msg, logger)
                    raw_file = raw_file.replace(logical_id, item_guid)

        return raw_file

    def _replace_parameters(self, file_obj: object, item_obj: object) -> str:
        """
        Replaces values found in parameter file with the chosen environment value. Handles two parameter dictionary structures.

        Args:
            file_obj: The File object instance that provides the file content and file path.
            item_obj: The Item object instance that provides the item type and item name.
        """
        from fabric_cicd._parameter._utils import (
            check_replacement,
            extract_find_value,
            extract_parameter_filters,
            extract_replace_value,
            process_environment_key,
            replace_key_value,
        )

        # Parse the file_obj and item_obj
        raw_file = file_obj.contents
        item_type = item_obj.type
        item_name = item_obj.name
        file_path = file_obj.file_path

        if "key_value_replace" in self.environment_parameter:
            for parameter_dict in self.environment_parameter.get("key_value_replace"):
                # Extract the file filter values and set the match condition
                input_type, input_name, input_path = extract_parameter_filters(self, parameter_dict)
                filter_match = check_replacement(input_type, input_name, input_path, item_type, item_name, file_path)

                # Perform replacement if condition is met and file contains valid JSON
                if filter_match and check_valid_json_content(raw_file):
                    raw_file = replace_key_value(self, parameter_dict, raw_file, self.environment)

        if "find_replace" in self.environment_parameter:
            for parameter_dict in self.environment_parameter.get("find_replace"):
                # Extract the file filter values and set the match condition
                input_type, input_name, input_path = extract_parameter_filters(self, parameter_dict)
                filter_match = check_replacement(input_type, input_name, input_path, item_type, item_name, file_path)

                # Extract the find_pattern and replace_value_dict
                find_info = extract_find_value(parameter_dict, raw_file, filter_match)
                replace_value_dict = process_environment_key(self, parameter_dict.get("replace_value", {}))

                # Replace any found references with specified environment value if conditions are met
                if filter_match and self.environment in replace_value_dict and find_info["has_matches"]:
                    replace_value = extract_replace_value(self, replace_value_dict[self.environment])
                    if replace_value:
                        pattern = find_info["pattern"]
                        is_regex = find_info["is_regex"]

                        if is_regex:
                            # For regex patterns, use re.sub with lambda to replace only the captured group
                            # Use string slicing to precisely replace only the captured group (group 1)
                            # The slicing calculates relative positions: match.start(1) - match.start(0) gives
                            # the start position of group 1 within the full match, and similarly for end position
                            raw_file = re.sub(
                                pattern,
                                lambda match, repl=replace_value: (
                                    match.group(0)[: match.start(1) - match.start(0)]
                                    + repl
                                    + match.group(0)[match.end(1) - match.start(0) :]
                                ),
                                raw_file,
                            )
                            logger.debug(
                                f"Replacing regex pattern '{pattern}' captured group with '{replace_value}' in {item_name}.{item_type}"
                            )
                        else:
                            # For non-regex matches, replace as before
                            raw_file = raw_file.replace(pattern, replace_value)
                            logger.debug(f"Replacing '{pattern}' with '{replace_value}' in {item_name}.{item_type}")

        return raw_file

    def _replace_workspace_ids(self, raw_file: str) -> str:
        """
        Replaces feature branch workspace ID, default (i.e. 00000000-0000-0000-0000-000000000000) and non-default
        (actual workspace ID guid) values, with target workspace ID in the raw file content.

        Args:
            raw_file: The raw file content where workspace IDs need to be replaced.
        """
        # Use re.sub to replace all matches
        return re.sub(
            constants.WORKSPACE_ID_REFERENCE_REGEX,
            lambda match: (
                match.group(0).replace(constants.DEFAULT_GUID, self.workspace_id)
                if match.group(2) == constants.DEFAULT_GUID
                else match.group(0)
            ),
            raw_file,
        )

    def _convert_id_to_name(self, item_type: str, generic_id: str, lookup_type: str) -> str:
        """
        For a given item_type and id, returns the item name. Special handling for both deployed and repository items.

        Args:
            item_type: Type of the item (e.g., Notebook, Environment).
            generic_id: Logical id or item guid of the item based on lookup_type.
            lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
        """
        lookup_dict = self.repository_items if lookup_type == "Repository" else self.deployed_items

        for item_details in lookup_dict[item_type].values():
            lookup_id = item_details.logical_id if lookup_type == "Repository" else item_details.guid
            if lookup_id == generic_id:
                return item_details.name
        # if not found
        return None

    def _convert_path_to_id(self, item_type: str, path: str) -> str:
        """
        For a given path and item type, returns the logical id.

        Args:
            item_type: Type of the item (e.g., Notebook, Environment).
            path: Full path of the desired item.
        """
        if item_type in self.repository_items:
            for item_details in self.repository_items[item_type].values():
                if item_details.path == Path(path):
                    return item_details.logical_id
        # if not found
        return None

    def _publish_item(
        self,
        item_name: str,
        item_type: str,
        exclude_path: str = r"^(?!.*)",
        func_process_file: Optional[callable] = None,
        **kwargs,
    ) -> None:
        """
        Publishes or updates an item in the Fabric Workspace.

        Args:
            item_name: Name of the item to publish.
            item_type: Type of the item (e.g., Notebook, Environment).
            exclude_path: Regex string of paths to exclude. Defaults to r"^(?!.*)".
            func_process_file: Custom function to process file contents. Defaults to None.
            **kwargs: Additional keyword arguments.
        """
        item = self.repository_items[item_type][item_name]

        # Initialize response collection for this item if responses are being tracked
        api_response = None

        # Skip publishing if the item is excluded by the regex
        if self.publish_item_name_exclude_regex:
            regex_pattern = check_regex(self.publish_item_name_exclude_regex)
            if regex_pattern.match(item_name):
                item.skip_publish = True
                logger.info(f"Skipping publishing of {item_type} '{item_name}' due to exclusion regex.")
                return

        # Skip publishing if the item's folder path is excluded by the regex
        if self.publish_folder_path_exclude_regex:
            regex_pattern = check_regex(self.publish_folder_path_exclude_regex)
            relative_path = item.path.relative_to(Path(self.repository_directory))
            relative_path_str = relative_path.as_posix()
            if regex_pattern.search(relative_path_str):
                item.skip_publish = True
                logger.info(f"Skipping publishing of {item_type} '{item_name}' due to folder path exclusion regex.")
                return

        # Skip publishing if the item is not in the include list
        if self.items_to_include:
            current_item = f"{item_name}.{item_type}"

            # Normalize include list to a lowercase set for efficient lookups
            normalized_include_set = {include_item.lower() for include_item in self.items_to_include}

            # Check for exact match or case-insensitive match
            match_found = current_item in self.items_to_include or current_item.lower() in normalized_include_set
            if not match_found:
                item.skip_publish = True
                logger.info(f"Skipping publishing of {item_type} '{item_name}' as it is not in the include list.")
                return

        item_guid = item.guid
        item_files = item.item_files

        metadata_body = {"displayName": item_name, "type": item_type}

        # Only shell deployment, no definition support
        shell_only_publish = item_type in constants.SHELL_ONLY_PUBLISH

        if kwargs.get("creation_payload"):
            creation_payload = {"creationPayload": kwargs["creation_payload"]}
            combined_body = {**metadata_body, **creation_payload}
        elif shell_only_publish:
            combined_body = metadata_body
        else:
            item_payload = []
            for file in item_files:
                if not re.match(exclude_path, file.relative_path):
                    if file.type == "text" and not str(file.file_path).endswith(".platform"):
                        file.contents = func_process_file(self, item, file) if func_process_file else file.contents
                        file.contents = self._replace_logical_ids(file.contents)
                        file.contents = self._replace_parameters(file, item)
                        file.contents = self._replace_workspace_ids(file.contents)

                    item_payload.append(file.base64_payload)

            definition_body = {"definition": {"parts": item_payload}}
            combined_body = {**metadata_body, **definition_body}

        logger.info(f"Publishing {item_type} '{item_name}'")

        is_deployed = bool(item_guid)

        if not is_deployed:
            combined_body = {**combined_body, **{"folderId": item.folder_id}}

            # Create a new item if it does not exist
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item
            item_create_response = self.endpoint.invoke(
                method="POST", url=f"{self.base_api_url}/items", body=combined_body
            )
            api_response = item_create_response
            item_guid = item_create_response["body"]["id"]
            self.repository_items[item_type][item_name].guid = item_guid

        elif is_deployed and not shell_only_publish:
            # Update the item's definition if full publish is required
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item-definition
            update_response = self.endpoint.invoke(
                method="POST",
                url=f"{self.base_api_url}/items/{item_guid}/updateDefinition?updateMetadata=True",
                body=definition_body,
            )
            api_response = update_response
        elif is_deployed and shell_only_publish:
            # Remove the 'type' key as it's not supported in the update-item API
            metadata_body.pop("type", None)

            # Update the item's metadata
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item
            metadata_update_response = self.endpoint.invoke(
                method="PATCH",
                url=f"{self.base_api_url}/items/{item_guid}",
                body=metadata_body,
            )
            api_response = metadata_update_response

        if "disable_workspace_folder_publish" not in constants.FEATURE_FLAG:
            deployed_item = self.deployed_items.get(item_type, {}).get(item_name) if is_deployed else None
            # Check if the folder has changed
            if deployed_item is not None and deployed_item.folder_id != item.folder_id:
                # Move the item to the correct folder if it has been moved
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/move-item
                move_response = self.endpoint.invoke(
                    method="POST",
                    url=f"{self.base_api_url}/items/{item_guid}/move",
                    body={"targetFolderId": f"{item.folder_id}"},
                )
                # For move operations, combine responses if we're tracking them
                if self.responses is not None:
                    if api_response:
                        # If we already have a response, combine them
                        api_response = {"publish_response": api_response, "move_response": move_response}
                    else:
                        # If move is the only operation, use the move response
                        api_response = move_response
                logger.debug(
                    f"Moved {item_guid} from folder_id {self.deployed_items[item_type][item_name].folder_id} to folder_id {item.folder_id}"
                )

        # Store response if responses are being tracked
        if self.responses is not None and api_response:
            # Initialize item_type dictionary if it doesn't exist
            if item_type not in self.responses:
                self.responses[item_type] = {}
            self.responses[item_type][item_name] = api_response

        # skip_publish_logging provided in kwargs to suppress logging if further processing is to be done
        if not kwargs.get("skip_publish_logging", False):
            logger.info(f"{constants.INDENT}Published")
        return

    def _unpublish_item(self, item_name: str, item_type: str) -> None:
        """
        Unpublishes an item from the Fabric workspace.

        Args:
            item_name: Name of the item to unpublish.
            item_type: Type of the item (e.g., Notebook, Environment).
        """
        item_guid = self.deployed_items[item_type][item_name].guid

        # Skip unpublishing if the item is not in the include list
        if self.items_to_include:
            current_item = f"{item_name}.{item_type}"

            # Normalize include list to a lowercase set for efficient lookups
            normalized_include_set = {include_item.lower() for include_item in self.items_to_include}

            # Check for exact match or case-insensitive match
            match_found = current_item in self.items_to_include or current_item.lower() in normalized_include_set
            if not match_found:
                logger.info(f"Skipping unpublishing of {item_type} '{item_name}' as it is not in the include list.")
                return

        logger.info(f"Unpublishing {item_type} '{item_name}'")

        # Delete the item from the workspace
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/delete-item
        try:
            self.endpoint.invoke(method="DELETE", url=f"{self.base_api_url}/items/{item_guid}")
            logger.info(f"{constants.INDENT}Unpublished")
        except Exception as e:
            logger.warning(f"Failed to unpublish {item_type} '{item_name}'.  Raw exception: {e}")

    def _refresh_deployed_folders(self) -> None:
        """
        Converts the folder list payload into a structure of folder name and their ids

        output should be like this:
        {
            "/Pipeline": "323eaa75-d70b-498c-8544-6c4219bf336e",
            "/Notebook": "f802fd90-c70e-4d77-b079-538f617646d3",
            "/Notebook/Processing": "36ed1a63-be82-4a7a-9364-2e4ff3a66b31"
        }

        """
        self.deployed_folders = {}
        request_url = f"{self.base_api_url}/folders"
        folders = []

        while request_url:
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/folders/list-folders
            response = self.endpoint.invoke(method="GET", url=request_url)

            # Handle cases where the response body is empty
            folder_response = response["body"].get("value", [])
            folders.extend(folder for folder in folder_response)

            request_url = response["header"].get("continuationUri", None)

        # Create a lookup table for folders by their ID
        folder_lookup = {folder["id"]: folder for folder in folders}

        # Build the folder hierarchy
        folder_hierarchy = {}

        def get_full_path(folder: dict) -> str:
            """Recursively build the full path for a folder"""
            parent_id = folder.get("parentFolderId")
            if parent_id:
                parent_folder = folder_lookup.get(parent_id)
                if parent_folder:
                    return f"{get_full_path(parent_folder)}/{folder['displayName']}"
            return f"/{folder['displayName']}"

        for folder in folders:
            full_path = get_full_path(folder)
            folder_hierarchy[full_path] = folder["id"]

        self.deployed_folders = folder_hierarchy

    def _refresh_repository_folders(self) -> None:
        """
        Converts the folder list payload into a structure of folder name and their ids,
        skipping empty folders or folders that only contain other empty folders.

        output should be like this:
        {
            "/Pipeline": "",
            "/Notebook": "",
            "/Notebook/Processing": ""
        }
        """
        self.repository_folders = {}

        root_path = Path(self.repository_directory)
        folder_hierarchy = {}

        # Collect all folders that directly contain a .platform file
        platform_folders = set(p.parent for p in root_path.rglob(".platform"))

        # Now, for every folder, check if any of its subfolders is in platform_folders
        for folder in root_path.rglob("*"):
            if not folder.is_dir() or folder == root_path or folder.name == ".children":
                continue

            # Skip folders that directly contain a .platform file
            if folder in platform_folders:
                continue

            # Check if any subfolder (at any depth) is in platform_folders
            if any(sub in platform_folders for sub in folder.rglob("*") if sub.is_dir()):
                relative_path = f"/{folder.relative_to(root_path).as_posix()}"
                folder_hierarchy[relative_path] = ""

        self.repository_folders = folder_hierarchy

    def _publish_folders(self) -> None:
        """Publishes all folders from the repository."""
        # Sort folders by the number of '/' in their paths (ascending order)
        sorted_folders = sorted(self.repository_folders.keys(), key=lambda path: path.count("/"))
        print_header("Publishing Workspace Folders")
        logger.info("Publishing Workspace Folders")
        for folder_path in sorted_folders:
            if folder_path in self.deployed_folders:
                # Folder already deployed, update local hierarchy
                self.repository_folders[folder_path] = self.deployed_folders[folder_path]
                logger.debug(f"Folder exists: {folder_path}")
                continue

            # Publish the folder
            folder_name = folder_path.split("/")[-1]
            folder_parent_path = "/".join(folder_path.split("/")[:-1])
            folder_parent_id = self.repository_folders.get(folder_parent_path, None)

            if re.search(constants.INVALID_FOLDER_CHAR_REGEX, folder_name):
                msg = f"Folder name '{folder_name}' contains invalid characters."
                raise InputError(msg, logger)

            request_body = {"displayName": folder_name}
            if folder_parent_id:
                request_body["parentFolderId"] = folder_parent_id

            request_url = f"{self.base_api_url}/folders"
            response = self.endpoint.invoke(method="POST", url=request_url, body=request_body)

            # Update local hierarchy with the new folder ID
            self.repository_folders[folder_path] = response["body"]["id"]
            logger.debug(f"Published folder: {folder_path}")

        logger.info(f"{constants.INDENT}Published")

    def _unpublish_folders(self) -> None:
        """Unpublishes all empty folders in workspace."""
        # Sort folders by the number of '/' in their paths (descending order)
        sorted_folder_ids = [
            self.deployed_folders[key]
            for key in sorted(self.deployed_folders.keys(), key=lambda path: path.count("/"), reverse=True)
        ]

        ## Any folder that neither contains items nor is an ancestor of a folder
        ## containing items is considered orphaned

        # Create a set of folders that contain items
        unorphaned_folders = {
            item.folder_id for items in self.deployed_items.values() for item in items.values() if item.folder_id
        }
        # Skip deletion if all deployed folders are unorphaned
        if unorphaned_folders == set(sorted_folder_ids):
            return

        # Create a reversed mapping for folder_id to folder_path lookups
        folder_id_to_path_mapping = {folder_id: folder_path for folder_path, folder_id in self.deployed_folders.items()}

        # Create a copy of the unorphaned_folders set to safely iterate while modifying the original set
        folder_lookup = unorphaned_folders.copy()

        # For each folder containing items, identify and protect all its ancestor folders from deletion
        for folder_id in folder_lookup:
            if folder_id in folder_id_to_path_mapping:
                # Get the folder path
                folder_path = folder_id_to_path_mapping[folder_id]

                # Move up the folder hierarchy and add all ancestor folders
                current_folder_path = folder_path
                while current_folder_path != "/":
                    # Get the parent folder path
                    current_folder_path = current_folder_path.rsplit("/", 1)[0] or "/"

                    # Get the folder_id for this path and add to the unorphaned_folder set
                    parent_folder_id = self.deployed_folders.get(current_folder_path)
                    if parent_folder_id:
                        unorphaned_folders.add(parent_folder_id)

        # Check if deletion can be skipped after update to unorphaned_folder set
        if unorphaned_folders == set(sorted_folder_ids):
            return

        logger.info("Unpublishing Workspace Folders")

        # Pop all folders

        for folder_id in sorted_folder_ids:
            if folder_id not in unorphaned_folders:
                # Folder deployed, but not in repository

                # Delete the folder from the workspace
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/folders/delete-folder
                try:
                    self.endpoint.invoke(method="DELETE", url=f"{self.base_api_url}/folders/{folder_id}")
                    logger.debug(f"Unpublished folder: {folder_id}")
                except Exception as e:
                    logger.warning(f"Failed to unpublish folder {folder_id}.  Raw exception: {e}")

        logger.info(f"{constants.INDENT}Unpublished")
