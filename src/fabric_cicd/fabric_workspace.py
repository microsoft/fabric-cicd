# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module provides the FabricWorkspace class to manage and publish workspace items to the Fabric API."""

import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_regex
from fabric_cicd._common._exceptions import ParameterFileError, ParsingError
from fabric_cicd._common._fabric_endpoint import FabricEndpoint
from fabric_cicd._common._item import Item
from fabric_cicd._common._logging import print_header

logger = logging.getLogger(__name__)


class FabricWorkspace:
    """A class to manage and publish workspace items to the Fabric API."""

    def __init__(
        self,
        workspace_id: str,
        repository_directory: str,
        item_type_in_scope: list[str],
        environment: str = "N/A",
        token_credential: TokenCredential = None,
        **kwargs,
    ) -> None:
        """
        Initializes the FabricWorkspace instance.

        Args:
            workspace_id: The ID of the workspace to interact with.
            repository_directory: Local directory path of the repository where items are to be deployed from.
            item_type_in_scope: Item types that should be deployed for a given workspace.
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
        )

        # Initialize endpoint
        self.endpoint = FabricEndpoint(
            # if credential is not defined, use DefaultAzureCredential
            token_credential=(
                DefaultAzureCredential() if token_credential is None else validate_token_credential(token_credential)
            )
        )

        # Validate and set class variables
        self.workspace_id = validate_workspace_id(workspace_id)
        self.repository_directory: Path = validate_repository_directory(repository_directory)
        self.item_type_in_scope = validate_item_type_in_scope(item_type_in_scope, upn_auth=self.endpoint.upn_auth)
        self.environment = validate_environment(environment)
        self.publish_item_name_exclude_regex = None
        self.repository_folders = {}
        self.repository_items = {}
        self.deployed_folders = {}
        self.deployed_items = {}

        # temporarily support base_api_url until deprecated
        if "base_api_url" in kwargs:
            logger.warning(
                """Setting base_api_url will be deprecated in a future version, please use the below moving forward:
                >>> import fabric_cicd.constants
                >>> constants.DEFAULT_API_ROOT_URL = '<your_base_api_url>'\n"""
            )
            self.base_api_url = f"{kwargs['base_api_url']}/v1/workspaces/{workspace_id}"
        else:
            self.base_api_url = f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}"

        # Initialize dictionaries to store repository and deployed items
        self._refresh_parameter_file()

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

    def _refresh_deployed_items(self) -> None:
        """Refreshes the deployed_items dictionary by querying the Fabric workspace items API."""
        # Get all items in workspace
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item
        response = self.endpoint.invoke(method="GET", url=f"{self.base_api_url}/items")

        self.deployed_items = {}

        for item in response["body"]["value"]:
            item_type = item["type"]
            item_description = item["description"]
            item_name = item["displayName"]
            item_guid = item["id"]
            item_folder_id = item.get("folderId", "")

            # Add an empty dictionary if the item type hasn't been added yet
            if item_type not in self.deployed_items:
                self.deployed_items[item_type] = {}

            # Add item details to the deployed_items dictionary
            self.deployed_items[item_type][item_name] = Item(
                type=item_type,
                name=item_name,
                description=item_description,
                guid=item_guid,
                folder_id=item_folder_id,
            )

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
            check_parameter_structure,
            check_replacement,
            process_input_path,
        )

        # Parse the file_obj and item_obj
        raw_file = file_obj.contents
        item_type = item_obj.type
        item_name = item_obj.name
        file_path = file_obj.file_path

        if "find_replace" in self.environment_parameter:
            structure_type = check_parameter_structure(self.environment_parameter, param_name="find_replace")
            msg = "Replacing {} with {} in {}.{}"

            # Handle new parameter file structure
            if structure_type == "new":
                for parameter_dict in self.environment_parameter["find_replace"]:
                    find_value = parameter_dict["find_value"]
                    replace_value = parameter_dict["replace_value"]
                    input_type = parameter_dict.get("item_type")
                    input_name = parameter_dict.get("item_name")
                    input_path = process_input_path(self.repository_directory, parameter_dict.get("file_path"))

                    # Perform replacement if a condition is met and replace any found references with specified environment value
                    if (find_value in raw_file and self.environment in replace_value) and check_replacement(
                        input_type, input_name, input_path, item_type, item_name, file_path
                    ):
                        raw_file = raw_file.replace(find_value, replace_value[self.environment])
                        logger.debug(msg.format(find_value, replace_value[self.environment], item_name, item_type))

            # Handle original parameter file structure
            # TODO: Deprecate old structure handling by April 24, 2025
            if structure_type == "old":
                for key, parameter_dict in self.environment_parameter["find_replace"].items():
                    if key in raw_file and self.environment in parameter_dict:
                        # replace any found references with specified environment value
                        raw_file = raw_file.replace(key, parameter_dict[self.environment])
                        logger.debug(msg.format(key, parameter_dict, item_name, item_type))

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
                match.group(0).replace(constants.DEFAULT_WORKSPACE_ID, self.workspace_id)
                if match.group(2) == constants.DEFAULT_WORKSPACE_ID
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
        # Skip publishing if the item is excluded by the regex
        if self.publish_item_name_exclude_regex:
            regex_pattern = check_regex(self.publish_item_name_exclude_regex)
            if regex_pattern.match(item_name):
                logger.info(f"Skipping publishing of {item_type} '{item_name}' due to exclusion regex.")
                return

        item = self.repository_items[item_type][item_name]
        item_guid = item.guid
        item_files = item.item_files

        max_retries = constants.MAX_RETRY_OVERRIDE.get(item_type, 5)

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
                    if file.type == "text":
                        file.contents = func_process_file(self, item, file) if func_process_file else file.contents
                        if not str(file.file_path).endswith(".platform"):
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
                method="POST", url=f"{self.base_api_url}/items", body=combined_body, max_retries=max_retries
            )
            item_guid = item_create_response["body"]["id"]
            self.repository_items[item_type][item_name].guid = item_guid

        elif is_deployed and not shell_only_publish:
            # Update the item's definition if full publish is required
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item-definition
            self.endpoint.invoke(
                method="POST",
                url=f"{self.base_api_url}/items/{item_guid}/updateDefinition?updateMetadata=True",
                body=definition_body,
                max_retries=max_retries,
            )
        elif is_deployed and shell_only_publish:
            # Remove the 'type' key as it's not supported in the update-item API
            metadata_body.pop("type", None)

            # Update the item's metadata
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item
            self.endpoint.invoke(
                method="PATCH",
                url=f"{self.base_api_url}/items/{item_guid}",
                body=metadata_body,
                max_retries=max_retries,
            )

        if "disable_workspace_folder_publish" not in constants.FEATURE_FLAG:  # noqa: SIM102
            if is_deployed and self.deployed_items[item_type][item_name].folder_id != item.folder_id:
                # Move the item to the correct folder if it has been moved
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/move-item
                self.endpoint.invoke(
                    method="POST",
                    url=f"{self.base_api_url}/items/{item_guid}/move",
                    body={"targetFolderId": f"{item.folder_id}"},
                    max_retries=max_retries,
                )
                logger.debug(
                    f"Moved {item.id} from folder_id {self.deployed_items[item_type][item_name].folder_id} to folder_id {item.folder_id}"
                )

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

    def _refresh_repository_folders(self) -> dict:
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

        # Walk through the directory structure
        for root, dirs, files in os.walk(root_path):
            folder = Path(root)
            if not folder.is_dir():
                continue

            # Check if a `.platform` file exists directly beneath the folder
            if ".platform" in files:
                # Skip this folder and its subfolders
                dirs.clear()
                continue

            # Check if any parent folder has already been excluded
            if any((Path(root).parent / ".platform").exists() for root in Path(root).parents if root != root_path):
                continue

            # Skip empty folders
            if not any(Path.iterdir(Path(root))):
                continue

            # Ensure the folder contains a subfolder, and that subfolder contains a `.platform` file
            subfolders = [subfolder for subfolder in folder.iterdir() if subfolder.is_dir()]
            if not any((subfolder / ".platform").exists() for subfolder in subfolders):
                continue

            # Build the relative path from the root and convert it to the desired format
            relative_path = f"/{Path(root).relative_to(root_path).as_posix()}"

            # Skip the root directory itself ("/.")
            if relative_path == "/.":
                continue

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
        """Unublishes all empty folders in workspace."""
        # Sort folders by the number of '/' in their paths (descending order)
        sorted_folder_ids = [
            self.deployed_folders[key]
            for key in sorted(self.deployed_folders.keys(), key=lambda path: path.count("/"), reverse=True)
        ]

        logger.info("Unpublishing Workspace Folders")

        ## any folder that is not in folderid_dict is an orphaned folder

        # Get folders with items
        deployed_folder_ids_with_items = []

        for items in self.deployed_items.values():
            for item in items.values():
                deployed_folder_ids_with_items.append(item.folder_id)

        # Pop all folders

        for folder_id in sorted_folder_ids:
            if folder_id not in deployed_folder_ids_with_items:
                # Folder deployed, but not in repository

                # Delete the folder from the workspace
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/folders/delete-folder
                try:
                    self.endpoint.invoke(method="DELETE", url=f"{self.base_api_url}/folders/{folder_id}")
                    logger.debug(f"Unpublished folder: {folder_id}")
                except Exception as e:
                    logger.warning(f"Failed to unpublish folder {folder_id}.  Raw exception: {e}")

        logger.info(f"{constants.INDENT}Unpublished")
