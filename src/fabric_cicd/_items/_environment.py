# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from pathlib import Path

import yaml

"""
Functions to process and deploy Environment item.
"""

logger = logging.getLogger(__name__)


def publish_environments(fabric_workspace_obj):
    """
    Publishes all environment items from the repository.

    Environments can only deploy the shell; compute and spark configurations are published separately.
    """
    item_type = "Environment"
    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        # Only deploy the shell for environments
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, full_publish=False)
        _publish_environment_metadata(fabric_workspace_obj, item_name=item_name)


def _publish_environment_metadata(fabric_workspace_obj, item_name):
    """
    Publishes compute settings and libraries for a given environment item.

    This process involves two steps:
    1. Updating the compute settings.
    2. Uploading/overwriting libraries to the environment.
    3. Deleting libraries in the environment that are not present in repository.
    4. Publishing the updated settings.

    :param item_name: Name of the environment item whose compute settings are to be published.
    """
    item_type = "Environment"
    item_path = fabric_workspace_obj.repository_items[item_type][item_name]["path"]
    item_guid = fabric_workspace_obj.repository_items[item_type][item_name]["guid"]

    # Read compute settings from YAML file
    with Path.open(Path(item_path, "Setting", "Sparkcompute.yml"), "r+", encoding="utf-8") as f:
        yaml_body = yaml.safe_load(f)

        # Update instance pool settings if present
        if "instance_pool_id" in yaml_body:
            pool_id = yaml_body["instance_pool_id"]
            if "spark_pool" in fabric_workspace_obj.environment_parameter:
                parameter_dict = fabric_workspace_obj.environment_parameter["spark_pool"]
                if pool_id in parameter_dict:
                    # replace any found references with specified environment value
                    yaml_body["instancePool"] = parameter_dict[pool_id]
                    del yaml_body["instance_pool_id"]

        yaml_body = _convert_environment_compute_to_camel(fabric_workspace_obj, yaml_body)
        # Update compute settings
        # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-compute/update-staging-settings
        fabric_workspace_obj.endpoint.invoke(
            method="PATCH",
            url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/sparkcompute",
            body=yaml_body,
        )
        logger.info("Updating spark settings")

    # Add libraries to environment, overwriting anything with the same name
    for library in ["CustomLibraries", "PublicLibraries"]:
        repo_library_path = Path(item_path, "Libraries", library)
        if repo_library_path.exists():
            for file_path in repo_library_path.iterdir():
                with file_path.open("rb") as f:
                    files = {"file": (file_path.name, file_path.open("rb"))}
                    # Upload libraries From Repo
                    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/upload-staging-library
                    fabric_workspace_obj.endpoint.invoke(
                        method="POST",
                        url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/libraries",
                        files=files,
                    )
                    logger.info(f"Uploading {file_path.name} to {library}")

    # Get published libraries
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/get-published-libraries
    response_environment = fabric_workspace_obj.endpoint.invoke(
        method="GET", url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/libraries"
    )
    logger.info("Getting environment libraries")
    library_files = set()
    response_public_libraries = response_environment['body'].get('environmentYml', '')
    response_custom_libraries = response_environment['body'].get('customLibraries', {})
    if response_public_libraries != '':
        library_files.add("environment.yml")
    for files in response_custom_libraries.values():
        for file in files:
            library_files.add(file)

    # Check for files in live environment that are not in the repository and delete them
    repo_library_files = set()
    for library in ["CustomLibraries", "PublicLibraries"]:
        repo_library_path = Path(item_path, "Libraries", library)
        if repo_library_path.exists():
            for file_path in repo_library_path.iterdir():
                repo_library_files.add(file_path.name)
    for file in library_files:
        if file not in repo_library_files:
            # Delete Libraries Not In Repo
            # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/delete-staging-library
            fabric_workspace_obj.endpoint.invoke(
                method="DELETE",
                url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/libraries?libraryToDelete={file}",
                body={}
            )
            logger.info(f"Deleted {file} from live environment")

    # Publish updated settings
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/spark-libraries/publish-environment
    fabric_workspace_obj.endpoint.invoke(
        method="POST", url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/publish"
    )
    logger.info("Published environment")


def _convert_environment_compute_to_camel(fabric_workspace_obj, input_dict):
    """
    Converts dictionary keys stored in snake_case to camelCase, except for 'spark_conf'.

    :param input_dict: Dictionary with snake_case keys.
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
