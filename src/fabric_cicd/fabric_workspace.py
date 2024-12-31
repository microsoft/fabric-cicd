from fabric_cicd._common._custom_print import (
    print_line,
    print_timestamp,
    print_sub_line,
)
from fabric_cicd._common._fabric_endpoint import FabricEndpoint
import os
import json
import base64
import yaml


class FabricWorkspace:
    """
    A class to manage and publish workspace items to the Fabric API.
    """

    def __init__(
        self,
        workspace_id: str,
        repository_directory: str,
        item_type_in_scope: list[str],
        base_api_url: str = "https://api.fabric.microsoft.com/",
        environment: str = "N/A",
        debug_output: bool = False,
    ) -> None:
        """
        Initializes the FabricWorkspace instance.

        :param workspace_id: The ID of the workspace to interact with.
        :param environment: The environment to be used for parameterization.
        :param repository_directory: Directory path where repository items are located.
        :param item_type_in_scope: Item types that should be deployed for given workspace.
        :param base_api_url: Base URL for the Fabric API. Defaults to the Fabric API endpoint.
        :param debug_output: If True, enables debug output for API requests.
        """
        from fabric_cicd._common._validate_input import (
            validate_workspace_id,
            validate_repository_directory,
            validate_item_type_in_scope,
            validate_base_api_url,
            validate_environment,
            validate_debug_output,
        )

        # validate and record
        self.workspace_id = validate_workspace_id(workspace_id)
        self.repository_directory = validate_repository_directory(repository_directory)
        self.item_type_in_scope = validate_item_type_in_scope(item_type_in_scope)
        self.base_api_url = (
            f"{validate_base_api_url(base_api_url)}/v1/workspaces/{workspace_id}"
        )

        self.environment = validate_environment(environment)
        self.debug_output = validate_debug_output(debug_output)

        # do work
        self.endpoint = FabricEndpoint(debug_output=self.debug_output)
        self._refresh_parameter_file()
        self._refresh_deployed_items()
        self._refresh_repository_items()

    def _refresh_parameter_file(self):
        # load parameters if file is present
        parameter_file_path = os.path.join(self.repository_directory, "parameter.yml")
        self.environment_parameter = {}

        if os.path.isfile(parameter_file_path):
            print_line(
                f"Info: Found parameter file '{parameter_file_path}'", color="yellow"
            )
            with open(parameter_file_path, "r") as yaml_file:
                self.environment_parameter = yaml.safe_load(yaml_file)

    def _refresh_repository_items(self):
        """
        Refreshes the repository_items dictionary by scanning the repository directory.
        """

        self.repository_items = {}

        for directory in os.scandir(self.repository_directory):
            if directory.is_dir():
                item_metadata_path = os.path.join(directory.path, ".platform")

                # Print a warning and skip directory if empty
                if not os.listdir(directory.path):
                    print_line(
                        f"Warning: directory {directory.name} is empty.", color="yellow"
                    )
                    continue

                # Attempt to read metadata file
                try:
                    with open(item_metadata_path, "r") as file:
                        item_metadata = json.load(file)
                except FileNotFoundError:
                    raise ValueError(
                        f"{item_metadata_path} path does not exist in the specified repository."
                    )
                except json.JSONDecodeError:
                    raise ValueError(f"Error decoding JSON in {item_metadata_path}.")

                # Ensure required metadata fields are present
                if (
                    "type" not in item_metadata["metadata"]
                    or "displayName" not in item_metadata["metadata"]
                ):
                    raise ValueError(
                        f"displayName & type are required in {item_metadata_path}"
                    )

                item_type = item_metadata["metadata"]["type"]
                item_description = item_metadata["metadata"].get("description", "")
                item_name = item_metadata["metadata"]["displayName"]
                item_logical_id = item_metadata["config"]["logicalId"]

                # Get the GUID if the item is already deployed
                item_guid = (
                    self.deployed_items.get(item_type, {})
                    .get(item_name, {})
                    .get("guid", "")
                )

                if item_type not in self.repository_items:
                    self.repository_items[item_type] = {}

                # Add the item to the repository_items dictionary
                self.repository_items[item_type][item_name] = {
                    "description": item_description,
                    "path": directory.path,
                    "guid": item_guid,
                    "logical_id": item_logical_id,
                }

    def _refresh_deployed_items(self):
        """
        Refreshes the deployed_items dictionary by querying the Fabric workspace items API.
        """
        # Get all items in workspace
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item
        response = self.endpoint.invoke(method="GET", url=f"{self.base_api_url}/items")

        self.deployed_items = {}

        for item in response["body"]["value"]:
            item_type = item["type"]
            item_description = item["description"]
            item_name = item["displayName"]
            item_guid = item["id"]

            # Add an empty dictionary if the item type hasn't been added yet
            if item_type not in self.deployed_items:
                self.deployed_items[item_type] = {}

            # Add item details to the deployed_items dictionary
            self.deployed_items[item_type][item_name] = {
                "description": item_description,
                "guid": item_guid,
            }

    def _replace_logical_ids(self, raw_file):
        """
        Replaces logical IDs with deployed GUIDs in the raw file content.

        :param raw_file: The raw file content where logical IDs need to be replaced.
        :return: The raw file content with logical IDs replaced by GUIDs.
        """
        for items in self.repository_items.values():
            for item_dict in items.values():
                logical_id = item_dict["logical_id"]
                item_guid = item_dict["guid"]

                if logical_id in raw_file:
                    if item_guid == "":
                        raise Exception(
                            "Cannot replace logical ID as referenced item is not yet deployed."
                        )
                    else:
                        raw_file = raw_file.replace(logical_id, item_guid)

        return raw_file

    def _replace_parameters(self, raw_file):
        """
        Replaces values found in parameter file with the chosen environment value.

        :param raw_file: The raw file content where parameter values need to be replaced.
        """
        if "find_replace" in self.environment_parameter:
            for key, parameter_dict in self.environment_parameter[
                "find_replace"
            ].items():
                if key in raw_file:
                    # if environment not found in dict
                    if self.environment in parameter_dict:
                        # replace any found references with specified environment value
                        raw_file = raw_file.replace(
                            key, parameter_dict[self.environment]
                        )

        return raw_file

    def _replace_activity_workspace_ids(self, raw_file, lookup_type):
        """
        Replaces feature branch workspace ID referenced in data pipeline activities with target workspace ID
        in the raw file content.

        :param raw_file: The raw file content where workspace IDs need to be replaced.
        :return: The raw file content with feature branch workspace IDs replaced by target workspace IDs.
        """
        # Create a dictionary from the raw_file
        item_content_dict = json.loads(raw_file)

        def _find_and_replace_activity_workspace_ids(input_object):
            """
            Recursively scans through JSON to find and replace feature branch workspace IDs in nested and
            non-nested activities where workspaceId
            property exists (e.g. Trident Notebook). Note: the function can be modified to process other pipeline
            activities where workspaceId exists.

            :param input_object: Object can be a dictionary or list present in the input JSON.
            """
            # Check if the current object is a dictionary
            if isinstance(input_object, dict):
                target_workspace_id = self.workspace_id

                # Iterate through the activities and search for TridentNotebook activities
                for key, value in input_object.items():
                    if key == "type" and value == "TridentNotebook":
                        # Convert the notebook ID to its name
                        item_type = "Notebook"
                        referenced_id = input_object["typeProperties"]["notebookId"]
                        referenced_name = self._convert_id_to_name(
                            item_type=item_type,
                            generic_id=referenced_id,
                            lookup_type=lookup_type,
                        )
                        # Replace workspace ID with target workspace ID if the referenced notebook exists in the repo
                        if referenced_name:
                            input_object["typeProperties"][
                                "workspaceId"
                            ] = target_workspace_id

                    # Recursively search in the value
                    else:
                        _find_and_replace_activity_workspace_ids(value)

            # Check if the current object is a list
            elif isinstance(input_object, list):
                # Recursively search in each item
                for item in input_object:
                    _find_and_replace_activity_workspace_ids(item)

        # Start the recursive search and replace from the root of the JSON data
        _find_and_replace_activity_workspace_ids(item_content_dict)

        # Convert the updated dict back to a JSON string
        raw_file = json.dumps(item_content_dict, indent=2)

        return raw_file

    def _convert_id_to_name(self, item_type, generic_id, lookup_type):
        """
        For a given item_type and id, returns the item name.  Special handling for both deployed and repository items

        :param item_type: Type of the item (e.g., Notebook, Environment).
        :param generic_id: Logical id or item guid of the item based on lookup_type.
        :param lookup_type: Finding references in deployed file or repo file (Deployed or Repository)
        """

        lookup_dict = (
            self.repository_items
            if lookup_type == "Repository"
            else self.deployed_items
        )
        lookup_key = "logical_id" if lookup_type == "Repository" else "guid"

        for item_name, item_details in lookup_dict[item_type].items():
            if item_details.get(lookup_key) == generic_id:
                return item_name
        # if not found
        return None

    def _publish_item(
        self, item_name, item_type, excluded_files={".platform"}, full_publish=True
    ):
        """
        Publishes or updates an item in the Fabric Workspace.

        :param item_name: Name of the item to publish.
        :param item_type: Type of the item (e.g., Notebook, Environment).
        :param excluded_files: Set of file names to exclude from the publish process.
        :param full_publish: If True, publishes the full item with its content. If False, only
            publishes metadata (for items like Environments).
        """
        item_path = self.repository_items[item_type][item_name]["path"]
        item_guid = self.repository_items[item_type][item_name]["guid"]
        item_description = self.repository_items[item_type][item_name]["description"]

        metadata_body = {
            "displayName": item_name,
            "type": item_type,
            "description": item_description,
        }

        if full_publish:
            item_payload = []
            for root, _, files in os.walk(item_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, item_path)

                    if file not in excluded_files:
                        with open(full_path, "r", encoding="utf-8") as f:
                            raw_file = f.read()

                        # Replace feature branch workspace IDs with target workspace IDs in data pipeline activities.
                        if item_type == "DataPipeline":
                            raw_file = self._replace_activity_workspace_ids(
                                raw_file, "Repository"
                            )

                        # Replace default workspace id with target workspace id
                        # TODO Remove this once bug is resolved in API
                        if item_type == "Notebook":
                            default_workspace_string = (
                                '"workspaceId": "00000000-0000-0000-0000-000000000000"'
                            )
                            target_workspace_string = (
                                f'"workspaceId": "{self.workspace_id}"'
                            )
                            raw_file = raw_file.replace(
                                default_workspace_string, target_workspace_string
                            )

                        # Replace logical IDs with deployed GUIDs.
                        replaced_raw_file = self._replace_logical_ids(raw_file)
                        replaced_raw_file = self._replace_parameters(replaced_raw_file)

                        byte_file = replaced_raw_file.encode("utf-8")
                        payload = base64.b64encode(byte_file).decode("utf-8")

                        item_payload.append(
                            {
                                "path": relative_path,
                                "payload": payload,
                                "payloadType": "InlineBase64",
                            }
                        )

            definition_body = {"definition": {"parts": item_payload}}
            combined_body = {**metadata_body, **definition_body}
        else:
            combined_body = metadata_body

        print_timestamp(f"Publishing {item_type} '{item_name}'")

        if not item_guid:
            # Create a new item if it does not exist
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/create-item
            item_create_response = self.endpoint.invoke(
                method="POST", url=f"{self.base_api_url}/items", body=combined_body
            )
            item_guid = item_create_response["body"]["id"]
            self.repository_items[item_type][item_name]["guid"] = item_guid
        else:
            if full_publish:
                # Update the item's definition if full publish is required
                # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item-definition
                self.endpoint.invoke(
                    method="POST",
                    url=f"{self.base_api_url}/items/{item_guid}/updateDefinition",
                    body=definition_body,
                )

            # Remove the 'type' key as it's not supported in the update-item API
            metadata_body.pop("type", None)

            # Update the item's metadata
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/update-item
            self.endpoint.invoke(
                method="PATCH",
                url=f"{self.base_api_url}/items/{item_guid}",
                body=metadata_body,
            )

        print_sub_line("Published")

    def _unpublish_item(self, item_name, item_type):
        """
        Unpublishes an item from the Fabric workspace.

        :param item_name: Name of the item to unpublish.
        :param item_type: Type of the item (e.g., Notebook, Environment).
        """
        item_guid = self.deployed_items[item_type][item_name]["guid"]

        print_timestamp(f"Unpublishing {item_type} '{item_name}'")

        # Delete the item from the workspace
        # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/delete-item
        self.endpoint.invoke(
            method="DELETE", url=f"{self.base_api_url}/items/{item_guid}"
        )
        print_sub_line("Unpublished")
