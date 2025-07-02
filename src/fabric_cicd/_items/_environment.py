# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Environment item."""

import logging
import os
import re
import urllib.parse
from pathlib import Path

import dpath
import yaml

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._fabric_endpoint import handle_retry

logger = logging.getLogger(__name__)


def publish_environments(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all environment items from the repository.

    Environments can only deploy the shell; compute and spark configurations are published separately.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    # Check for ongoing publish
    check_environment_publish_state(fabric_workspace_obj, True)

    item_type = "Environment"
    for item_name, item in fabric_workspace_obj.repository_items.get(item_type, {}).items():
        # Only deploy the shell for environments
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            skip_publish_logging=True,
        )
        if item.skip_publish:
            continue
        _publish_environment_metadata(fabric_workspace_obj, item_name=item_name)


def _publish_environment_metadata(fabric_workspace_obj: FabricWorkspace, item_name: str) -> None:
    """
    Publishes compute settings and libraries for a given environment item.

    This process involves two steps:
    1. Check for ongoing publish.
    2. Updating the compute settings.
    3. Uploading/overwrite libraries to the environment.
    4. Delete libraries in the environment that are not present in repository.
    5. Publish the updated settings.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_name: Name of the environment item whose compute settings are to be published.
    """
    item_type = "Environment"
    item_path = fabric_workspace_obj.repository_items[item_type][item_name].path
    item_guid = fabric_workspace_obj.repository_items[item_type][item_name].guid

    # Update compute settings
    _update_compute_settings(fabric_workspace_obj, item_path, item_guid, item_name)

    repo_library_files = _get_repo_libraries(item_path)

    # Add libraries to environment, overwriting anything with the same name and return the list of libraries
    _add_libraries(fabric_workspace_obj, item_guid, repo_library_files)

    # Remove libraries from live environment that are not in the repository
    _remove_libraries(fabric_workspace_obj, item_guid, repo_library_files)

    # Publish updated settings
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/publish-environment
    fabric_workspace_obj.endpoint.invoke(
        method="POST", url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/publish"
    )

    logger.info(f"{constants.INDENT}Publish Submitted")


def check_environment_publish_state(fabric_workspace_obj: FabricWorkspace, initial_check: bool = False) -> None:
    """
    Checks the publish state of environments after deployment

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        initial_check: Flag to ignore publish failures on initial check.
    """
    ongoing_publish = True
    iteration = 1

    environments = fabric_workspace_obj.repository_items.get("Environment", {})

    filtered_environments = [
        k
        for k in environments
        if not fabric_workspace_obj.publish_item_name_exclude_regex
        or not re.search(fabric_workspace_obj.publish_item_name_exclude_regex, k)
    ]

    logger.info(f"Checking Environment Publish State for {filtered_environments}")

    while ongoing_publish:
        ongoing_publish = False

        response_state = fabric_workspace_obj.endpoint.invoke(
            method="GET", url=f"{fabric_workspace_obj.base_api_url}/environments/"
        )

        for item in response_state["body"]["value"]:
            item_name = item["displayName"]
            item_state = dpath.get(item, "properties/publishDetails/state", default="").lower()
            if item_name in environments and item_state == "running":
                ongoing_publish = True
            elif item_state in ["failed", "cancelled"] and not initial_check:
                msg = f"Publish {item_state} for {item_name}"
                raise Exception(msg)

        if ongoing_publish:
            handle_retry(
                attempt=iteration,
                base_delay=5,
                response_retry_after=120,
                prepend_message=f"{constants.INDENT}Operation in progress.",
            )
            iteration += 1

    if not initial_check:
        logger.info(f"{constants.INDENT}Published.")


def _update_compute_settings(
    fabric_workspace_obj: FabricWorkspace, item_path: Path, item_guid: str, item_name: str
) -> None:
    """
    Update spark compute settings.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_path: The path to the environment item.
        item_guid: The GUID of the environment item.
        item_name: Name of the environment item.
    """
    # Read compute settings from YAML file
    with Path.open(Path(item_path, "Setting", "Sparkcompute.yml"), "r+", encoding="utf-8") as f:
        yaml_body = yaml.safe_load(f)

        # Update instance pool settings if present
        if "instance_pool_id" in yaml_body:
            pool_id = yaml_body["instance_pool_id"]
            if "spark_pool" in fabric_workspace_obj.environment_parameter:
                parameter_dict = fabric_workspace_obj.environment_parameter["spark_pool"]
                for key in parameter_dict:
                    instance_pool_id = key["instance_pool_id"]
                    replace_value = key["replace_value"]
                    input_name = key.get("item_name")
                    if instance_pool_id == pool_id and (input_name == item_name or not input_name):
                        # replace any found references with specified environment value
                        yaml_body["instancePool"] = replace_value[fabric_workspace_obj.environment]
                        del yaml_body["instance_pool_id"]

        yaml_body = _convert_environment_compute_to_camel(fabric_workspace_obj, yaml_body)

        # Update compute settings
        # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-compute/update-staging-settings
        fabric_workspace_obj.endpoint.invoke(
            method="PATCH",
            url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/sparkcompute",
            body=yaml_body,
        )
        logger.info(f"{constants.INDENT}Updated Spark Settings")


def _get_repo_libraries(item_path: Path) -> dict:
    """
    Add libraries to environment, overwriting anything with the same name and returns a list of the libraries in the repo.

    Args:
        item_path: The path to the environment item.
    """
    repo_library_files = {}

    repo_library_path = Path(item_path, "Libraries")
    if repo_library_path.exists():
        for root, _dirs, files in os.walk(repo_library_path):
            for file_name in files:
                repo_library_files[file_name] = Path(root, file_name)

    return repo_library_files


def _add_libraries(fabric_workspace_obj: FabricWorkspace, item_guid: str, repo_library_files: dict) -> None:
    """
    Add libraries to environment, overwriting anything with the same name.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_guid: The GUID of the environment item.
        repo_library_files: The list of libraries in the repository.
    """
    for file_name, file_path in repo_library_files.items():
        library_file = {"file": (file_name, file_path.open("rb"))}

        # Upload libraries From Repo
        # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/upload-staging-library
        fabric_workspace_obj.endpoint.invoke(
            method="POST",
            url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/libraries",
            files=library_file,
        )
        logger.info(f"{constants.INDENT}Updated Library {file_path.name}")


def _remove_libraries(fabric_workspace_obj: FabricWorkspace, item_guid: str, repo_library_files: dict) -> None:
    """
    Remove libraries not in repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_guid: The GUID of the environment item.
        repo_library_files: The list of libraries in the repository.

    """
    # Get staged libraries
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/get-staging-libraries
    response_environment = fabric_workspace_obj.endpoint.invoke(
        method="GET", url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/libraries"
    )

    if response_environment["body"].get("errorCode", "") != "EnvironmentLibrariesNotFound":
        if (
            "environmentYml" in response_environment["body"]
            and response_environment["body"]["environmentYml"]  # not none or ''
            and "environment.yml" not in repo_library_files
        ):
            _remove_library(fabric_workspace_obj, item_guid, "environment.yml")

        custom_libraries = response_environment["body"].get("customLibraries", None)
        if custom_libraries:
            for files in custom_libraries.values():
                for file in files:
                    if file not in repo_library_files:
                        _remove_library(fabric_workspace_obj, item_guid, file)


def _remove_library(fabric_workspace_obj: FabricWorkspace, item_guid: str, file_name: str) -> None:
    """Remove library from workspace environment.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_guid: The GUID of the environment item.
        file_name: The name of the file to be removed.
    """
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/delete-staging-library
    # encode the URL to escape string to be URL-safe.
    file_name_encoded = urllib.parse.quote(file_name)
    fabric_workspace_obj.endpoint.invoke(
        method="DELETE",
        url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/libraries?libraryToDelete={file_name_encoded}",
        body={},
    )
    logger.info(f"{constants.INDENT}Removed {file_name}")


def _convert_environment_compute_to_camel(fabric_workspace_obj: FabricWorkspace, input_dict: dict) -> dict:
    """
    Converts dictionary keys stored in snake_case to camelCase, except for 'spark_conf'.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        input_dict: Dictionary with snake_case keys.
    """
    new_input_dict = {}

    for key, value in input_dict.items():
        if key == "spark_conf":
            new_key = "sparkProperties"
        else:
            # Convert the key to camelCase
            key_components = key.split("_")
            # Capitalize the first letter of each component except the first one
            new_key = key_components[0] + "".join(x.title() for x in key_components[1:])

        # Recursively update dictionary values if they are dictionaries
        if isinstance(value, dict):
            value = _convert_environment_compute_to_camel(fabric_workspace_obj, value)

        # Add the new key-value pair to the new dictionary
        new_input_dict[new_key] = value

    return new_input_dict
