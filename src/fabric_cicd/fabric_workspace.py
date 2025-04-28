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
import yaml
from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from fabric_cicd._common._exceptions import ParsingError
from fabric_cicd._common._fabric_endpoint import FabricEndpoint
from fabric_cicd._common._item import Item
from fabric_cicd._common._powerbi_endpoint import PowerBiEndpoint

logger = logging.getLogger(__name__)


class FabricWorkspace:
    """A class to manage and publish workspace items to the Fabric API."""

    ACCEPTED_ITEM_TYPES_UPN = ("DataPipeline", "Environment", "Notebook", "Report", "SemanticModel", "Lakehouse")
    ACCEPTED_ITEM_TYPES_NON_UPN = ("Environment", "Notebook", "Report", "SemanticModel", "Lakehouse")

    def __init__(
        self,
        workspace_id: str,
        repository_directory: str,
        item_type_in_scope: list[str],
        base_api_url: str = "https://api.fabric.microsoft.com/",
        base_api_url_powerbi: str = "https://api.powerbi.com/",
        environment: str = "N/A",
        token_credential: TokenCredential = None,
        token_credential_powerbi: TokenCredential = None,
    ) -> None:
        """
        Initializes the FabricWorkspace instance.

        Args:
            workspace_id: The ID of the workspace to interact with.
            repository_directory: Local directory path of the repository where items are to be deployed from.
            item_type_in_scope: Item types that should be deployed for a given workspace.
            base_api_url: Base URL for the Fabric API. Defaults to the Fabric API endpoint.
            environment: The environment to be used for parameterization.
            token_credential: The token credential to use for API requests.

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
            ...     base_api_url="https://orgapi.fabric.microsoft.com",
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
            validate_base_api_url,
            validate_base_api_url_powerbi,
            validate_environment,
            validate_item_type_in_scope,
            validate_repository_directory,
            validate_token_credential,
            validate_workspace_id,
        )

        # duplicate credentails object for Power BI REST API
        token_credential_powerbi = token_credential

        # Initialize endpoint
        self.endpoint = FabricEndpoint(
            # if credential is not defined, use DefaultAzureCredential
            token_credential=(
                DefaultAzureCredential() if token_credential is None else validate_token_credential(token_credential)
            )
        )

        self.endpoint_powerbi = PowerBiEndpoint(
            # if credential is not defined, use DefaultAzureCredential
            token_credential_powerbi=(
                DefaultAzureCredential()
                if token_credential_powerbi is None
                else validate_token_credential(token_credential_powerbi)
            )
        )

        # Validate and set class variables
        self.workspace_id = validate_workspace_id(workspace_id)
        self.repository_directory: Path = validate_repository_directory(repository_directory)
        self.item_type_in_scope = validate_item_type_in_scope(item_type_in_scope, upn_auth=self.endpoint.upn_auth)
        self.base_api_url = f"{validate_base_api_url(base_api_url)}/v1/workspaces/{workspace_id}"
        self.base_api_url_powerbi = (
            f"{validate_base_api_url_powerbi(base_api_url_powerbi)}v1.0/myorg/groups/{workspace_id}"
        )

        self.environment = validate_environment(environment)

        # Initialize dictionaries to store repository and deployed items
        self._refresh_parameter_file()
        self._refresh_deployed_items()
        self._refresh_repository_items()

    def _refresh_parameter_file(self) -> None:
        """Load parameters if file is present."""
        parameter_file_path = self.repository_directory / "parameter.yml"
        self.environment_parameter = {}

        if parameter_file_path.is_file():
            logger.info(f"Found parameter file '{parameter_file_path}'")
            with parameter_file_path.open(encoding="utf-8") as yaml_file:
                self.environment_parameter = yaml.safe_load(yaml_file)

    def _refresh_repository_items(self) -> None:
        """Refreshes the repository_items dictionary by scanning the repository directory."""
        self.repository_items = {}

        for root, _dirs, files in os.walk(self.repository_directory):
            directory = Path(root)
            # valid item directory with .platform file within
            if ".platform" in files:
                item_metadata_path = directory / ".platform"

                # Print a warning and skip directory if empty
                if not os.listdir(directory):
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
                item_path = Path(directory)

                # Get the GUID if the item is already deployed
                item_guid = self.deployed_items.get(item_type, {}).get(item_name, Item("", "", "", "")).guid

                if item_type not in self.repository_items:
                    self.repository_items[item_type] = {}

                # Add the item to the repository_items dictionary
                self.repository_items[item_type][item_name] = Item(
                    item_type, item_name, item_description, item_guid, item_logical_id, item_path
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

            # Add an empty dictionary if the item type hasn't been added yet
            if item_type not in self.deployed_items:
                self.deployed_items[item_type] = {}

            # Add item details to the deployed_items dictionary
            self.deployed_items[item_type][item_name] = Item(item_type, item_name, item_description, item_guid)

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

    def _replace_parameters(self, raw_file: str) -> str:
        """
        Replaces values found in parameter file with the chosen environment value.

        Args:
            raw_file: The raw file content where parameter values need to be replaced.
        """
        if "find_replace" in self.environment_parameter:
            for key, parameter_dict in self.environment_parameter["find_replace"].items():
                if key in raw_file and self.environment in parameter_dict:
                    # replace any found references with specified environment value
                    raw_file = raw_file.replace(key, parameter_dict[self.environment])
                    logger.debug(f"Replaced '{key}' with '{parameter_dict[self.environment]}'")

        return raw_file

    def _replace_semanticmodel_parameters(self, raw_file: str) -> str:
        """
        Replaces values found in parameter file with the chosen environment value.

        Args:
            raw_file: The raw file content where parameter values need to be replaced.
        """
        if "semanticmodel_parameters" in self.environment_parameter:
            for key, parameter_dict in self.environment_parameter["semanticmodel_parameters"].items():
                pattern_str = "expression\\s*" + key + '\\s*\\=\\s*"[^"()]*"\\s*meta\\s*\\[IsParameterQuery\\s=\\strue'
                replace_str = (
                    "expression " + key + ' = "' + parameter_dict[self.environment] + '" meta [IsParameterQuery=true'
                )
                raw_file = re.sub(pattern_str, replace_str, raw_file)
                logger.debug(f"Replaced '{key}' with '{parameter_dict[self.environment]}'")

        return raw_file

    def _get_semanticmodel_connections(self, item_guid: str) -> str:
        """
        Gets all data source connections for the semantic model

        Args:
            raw_file: The raw file content where parameter values need to be replaced.
        """
        response = self.endpoint_powerbi.invoke(
            method="GET", url=f"{self.base_api_url_powerbi}/datasets/{item_guid}/datasources"
        )

        return raw_file

    def _replace_workspace_ids(self, raw_file: str, item_type: str) -> str:
        """
        Replaces feature branch workspace ID, default (i.e. 00000000-0000-0000-0000-000000000000) and non-default
        (actual workspace ID guid) values, with target workspace ID in the raw file content.

        Args:
            raw_file: The raw file content where workspace IDs need to be replaced.
            item_type: Type of item where the replacement occurs (e.g., Notebook, DataPipeline).
        """
        # Replace all instances of default feature branch workspace ID with target workspace ID
        target_workspace_id = self.workspace_id
        default_workspace_string = '"workspaceId": "00000000-0000-0000-0000-000000000000"'
        target_workspace_string = f'"workspaceId": "{target_workspace_id}"'

        if default_workspace_string in raw_file:
            raw_file = raw_file.replace(default_workspace_string, target_workspace_string)

        # For DataPipeline item, additional replacements may be required
        if item_type == "DataPipeline":
            raw_file = self._replace_activity_workspace_ids(raw_file, target_workspace_id)

        return raw_file

    def _replace_activity_workspace_ids(self, raw_file: str, target_workspace_id: str) -> str:
        """
        Replaces all instances of non-default feature branch workspace IDs (actual guid of feature branch workspace)
        with target workspace ID found in DataPipeline activities.

        Args:
            raw_file: The raw file content where workspace IDs need to be replaced.
            target_workspace_id: The target workspace ID to replace with.
        """
        # Create a dictionary from the raw file
        item_content_dict = json.loads(raw_file)
        guid_pattern = re.compile(r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$")

        # Activities mapping dictionary: {Key: activity_name, Value: [item_type, item_id_name]}
        activities_mapping = {"RefreshDataflow": ["Dataflow", "dataflowId"]}

        # dpath library finds and replaces feature branch workspace IDs found in all levels of activities in the dictionary
        for path, activity_value in dpath.search(item_content_dict, "**/type", yielded=True):
            if activity_value in activities_mapping:
                # Split the path into components, create a path to 'workspaceId' and get the workspace ID value
                path = path.split("/")
                workspace_id_path = (*path[:-1], "typeProperties", "workspaceId")
                workspace_id = dpath.get(item_content_dict, workspace_id_path)

                # Check if the workspace ID is a valid GUID and is not the target workspace ID
                if guid_pattern.match(workspace_id) and workspace_id != target_workspace_id:
                    item_type, item_id_name = activities_mapping[activity_value]
                    # Create a path to the item's ID and get the item ID value
                    item_id_path = (*path[:-1], "typeProperties", item_id_name)
                    item_id = dpath.get(item_content_dict, item_id_path)
                    # Convert the item ID to a name to check if it exists in the repository
                    item_name = self._convert_id_to_name(
                        item_type=item_type, generic_id=item_id, lookup_type="Repository"
                    )
                    # If the item exists, the associated workspace ID is a feature branch workspace ID and will get replaced
                    if item_name:
                        dpath.set(item_content_dict, workspace_id_path, target_workspace_id)

        # Convert the updated dict back to a JSON string
        return json.dumps(item_content_dict, indent=2)

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

    def _convert_name_to_id(self, item_type: str, generic_name: str, lookup_type: str) -> str:
        """
        For a given item_type and id, returns the item name. Special handling for both deployed and repository items.

        Args:
            item_type: Type of the item (e.g., Notebook, Environment).
            generic_id: Logical id or item guid of the item based on lookup_type.
            lookup_type: Finding references in deployed file or repo file (Deployed or Repository).
        """
        lookup_dict = self.repository_items if lookup_type == "Repository" else self.deployed_items

        for item_details in lookup_dict[item_type].values():
            if item_details.name == generic_name:
                if item_details.guid:
                    return item_details.guid
                if item_details.logical_id:
                    return item_details.logical_id
                return None

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
        item = self.repository_items[item_type][item_name]
        item_guid = item.guid
        item_files = item.item_files

        max_retries = 10 if item_type == "SemanticModel" else 5

        metadata_body = {"displayName": item_name, "type": item_type}

        # Only shell deployment, no definition support
        shell_only_publish = item_type in ["Environment", "Lakehouse"]

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
                            file.contents = self._replace_parameters(file.contents)
                        if str(file.name) == "expressions.tmdl":
                            file.contents = self._replace_semanticmodel_parameters(file.contents)

                    item_payload.append(file.base64_payload)

            definition_body = {"definition": {"parts": item_payload}}
            combined_body = {**metadata_body, **definition_body}

        logger.info(f"Publishing {item_type} '{item_name}'")

        is_deployed = bool(item_guid)

        if not is_deployed:
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

        # skip_publish_logging provided in kwargs to suppress logging if further processing is to be done
        if not kwargs.get("skip_publish_logging", False):
            logger.info("Published")

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
            logger.info("Unpublished")
        except Exception as e:
            logger.warning(f"Failed to unpublish {item_type} '{item_name}'.  Raw exception: {e}")

    def _takeover_semanticmodel(self, item_guid: str) -> None:
        """
        Perform a ownership takeover on the sematic model.

        Args:
            item_guid: GUID of the semantic model
        """
        self.endpoint_powerbi.invoke(
            method="POST", url=f"{self.base_api_url_powerbi}/datasets/{item_guid}/Default.TakeOver", body=""
        )
        logger.info("Takeover Semantic model")

    def _keys_to_casefold(dict1: dict) -> dict:
        dict2 = {}
        for k, vals in dict1.items():
            if isinstance(dict1[k], dict):
                dict2[k] = FabricWorkspace._keys_to_casefold(dict1[k])
            else:
                dict2[k] = vals.casefold()
        return dict2

    def _get_semanticmodel_connections(self, item_guid: str) -> str:
        """
        Get connections of the sematic model.

        Args:
            item_guid: GUID of the semantic model
        """
        response = self.endpoint_powerbi.invoke(
            method="GET", url=f"{self.base_api_url_powerbi}/datasets/{item_guid}/datasources"
        )

        connections = response["body"]["value"]
        # gatewayconnections = [c for c in connections if c["gatewayType"] != "Personal"]
        for c in connections:
            if "connectionDetails" in c:
                if isinstance(c["connectionDetails"], str):
                    c["connectionDetails"] = json.loads(c["connectionDetails"])
                if "sharePointSiteUrl" in c["connectionDetails"]:
                    c["connectionDetails"]["url"] = c["connectionDetails"]["sharePointSiteUrl"]
                    del c["connectionDetails"]["sharePointSiteUrl"]
                # remove trailing "/" as there is some inconsistency in storing the url formats
                if "url" in c["connectionDetails"]:
                    c["connectionDetails"]["url"] = re.sub("\\/$", "", c["connectionDetails"]["url"])

                # casefold for all key to case-insensitive compare
                c["connectionDetails"] = FabricWorkspace._keys_to_casefold(c["connectionDetails"])

        return connections

    def _get_existing_connections(self) -> str:
        """
        Get all existing connections on the gateways.
        """
        # Found on: https://learn.microsoft.com/en-us/power-bi/connect-data/service-connect-cloud-data-sources

        baseurl = "https://api.powerbi.com/v2.0/myorg/me/"
        url = baseurl + "gatewayClusterDatasources?$expand=users"
        response = self.endpoint_powerbi.invoke(method="GET", url=url)

        # Filter out all personal connections.
        # Personal connections can not be shared with other users
        # this complicates maintaining and debugging the connection
        # A shared cloud connections is on the "TenantCloud" gateway
        connections = response["body"]["value"]

        gatewayconnections = []
        for c in connections:
            if c["gatewayType"] != "Personal":
                if "connectionDetails" in c:
                    if isinstance(c["connectionDetails"], str):
                        c["connectionDetails"] = json.loads(c["connectionDetails"])
                    if "sharePointSiteUrl" in c["connectionDetails"]:
                        c["connectionDetails"]["url"] = c["connectionDetails"]["sharePointSiteUrl"]
                        del c["connectionDetails"]["sharePointSiteUrl"]
                    # remove trailing "/" as there is some inconsistency in storing the url formats
                    if "url" in c["connectionDetails"]:
                        c["connectionDetails"]["url"] = re.sub("\\/$", "", c["connectionDetails"]["url"])
                    # casefold for all key to case-insensitive compare
                    c["connectionDetails"] = FabricWorkspace._keys_to_casefold(c["connectionDetails"])
                # Add Key clusterName for TenantCloud connections for easy filtering later on
                # and set clusterId to "00000000-0000-0000-0000-000000000000".
                # This is the GUID needed in the REST API to connect to a shared cloud datasource
                if c["gatewayType"] == "TenantCloud":
                    c["clusterName"] = "TenantCloud"
                    c["clusterId"] = "00000000-0000-0000-0000-000000000000"
                gatewayconnections.append(c)

        return gatewayconnections

    def _get_gateways(self) -> str:
        """
        Get all existing connections on the gateways for the choosen environment.
        """
        gateways = []
        if "gateways" in self.environment_parameter:
            for item in self.environment_parameter["gateways"].items():
                for i in item[1]:
                    # gateway = [i for i in item[1] if i == self.environment]
                    if i == self.environment:
                        gateways.append(item[0])

        return gateways

    def _set_semanticmodel_connections(self, item_guid: str, metadata_body: str) -> None:
        """
        Perform a ownership takeover on the sematic model.

        Args:
            item_guid: GUID of the semantic model
        """
        self.endpoint_powerbi.invoke(
            method="POST",
            url=f"{self.base_api_url_powerbi}/datasets/{item_guid}/Default.BindToGateway",
            body=metadata_body,
        )
        logger.info("Sematic model binded to gateway")

    def _get_refreshschedule(self, item_name: str) -> str:
        """
        Get connections of the sematic model.

        Args:
            item_name: name of the semantic model
        """
        # first find the schedule on the sematic model name
        refreshschedule = None
        if "refreshschedules" in self.environment_parameter:
            for item in self.environment_parameter["refreshschedules"]:
                if item_name in item["datasets"]:
                    refreshschedule = item["value"]
                    break
                # Not found on name, look for the default label
            if refreshschedule == None:
                for item in self.environment_parameter["refreshschedules"]:
                    if "default" in item["datasets"]:
                        refreshschedule = {}
                        refreshschedule["value"] = item["value"].copy()
                        break

        return refreshschedule

    def _get_isrefreshable(self, item_guid: str) -> str:
        """
        Gets the isRefreshable property of the semantic model.

        Args:
            item_guid: GUID of the semantic model
        """
        dataset = self.endpoint_powerbi.invoke(method="GET", url=f"{self.base_api_url_powerbi}/datasets/{item_guid}")
        # The API to be used depends on the isRefreshable property of the dataset

        return dataset["body"]["isRefreshable"] == True

    def _update_refreshschedule(self, item_guid: str, metadata_body: str) -> None:
        """
        Performs an update of the refresh schedule of on a semantic model.

        Args:
            item_guid: GUID of the semantic model
        """
        # The API to be used depends on the isRefreshable property of the dataset
        if self._get_isrefreshable(item_guid=item_guid):
            self.endpoint_powerbi.invoke(
                method="PATCH",
                url=f"{self.base_api_url_powerbi}/datasets/{item_guid}/refreshSchedule",
                body=metadata_body,
            )
            # Remove some elements from json object.
            # These are not valid for the API call directQueryRefreshSchedule
        else:
            if "enabled" in metadata_body["value"]:
                del metadata_body["value"]["enabled"]
            if "notifyOption" in metadata_body["value"]:
                del metadata_body["value"]["notifyOption"]

            self.endpoint_powerbi.invoke(
                method="PATCH",
                url=f"{self.base_api_url_powerbi}/datasets/{item_guid}/directQueryRefreshSchedule",
                body=metadata_body,
            )

        logger.info("refresh schedule updated")

    def _invoke_refresh(self, item_guid: str, max_retries: str) -> None:
        """
        Initiates the refresh of a semantic model.

        Args:
            item_guid: GUID of the semantic model
            max_retries: Max number of retries. When reaching the max, the refresh will continue but
            the procedure will not wait any longer.

            Use the parameter "dataset_refresh_nowait" and "dataset_refresh_norefresh" to skip waiting
            or skip refreshing the sematic model
        """
        logger.info("Starting Semantic model data refresh")

        metadata_body = {"notifyOption": "NoNotification"}

        self.endpoint_powerbi.invoke(
            method="POST",
            url=f"{self.base_api_url_powerbi}/datasets/{item_guid}/refreshes",
            body=metadata_body,
            max_retries=max_retries,
        )
