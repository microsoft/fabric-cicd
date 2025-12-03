# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Environment item."""

import logging
import re
from pathlib import Path

import dpath
import yaml

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


def publish_environments(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all environment items from the repository.

    Environments are deployed using the updateDefinition API, and then compute settings and libraries are published separately.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    logger.warning("The underlying legacy Microsoft Fabric Environment APIs will be deprecated by March 1, 2026.")
    logger.warning(
        "Please upgrade to the latest fabric-cicd version before March 1, 2026 to prevent broken Environment item deployments."
    )

    # Check for ongoing publish
    check_environment_publish_state(fabric_workspace_obj, True)

    item_type = "Environment"
    for item_name, item in fabric_workspace_obj.repository_items.get(item_type, {}).items():
        # Only deploy the environment shell when it just contains spark compute settings
        is_shell_only = set_environment_deployment_type(item)
        logger.debug(f"Environment '{item_name}'; shell_only deployment: {is_shell_only}")

        # Exclude Sparkcompute.yml from environment definition deployment (requires special handling)
        exclude_path = r"\Setting"
        fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=item_type,
            exclude_path=exclude_path,
            skip_publish_logging=True,
            shell_only_publish=is_shell_only,
        )
        if item.skip_publish:
            continue
        _publish_environment_metadata(fabric_workspace_obj, item_name)


def set_environment_deployment_type(item: Item) -> bool:
    """
    Return True if this Environment deployment should be treated as "shell-only".

    Shell-only when `Setting/Sparkcompute.yml` exists and there are no other
    non-`.platform` repository files under the environment root.

    Args:
        item: The Item object representing the Environment.
    """
    root = Path(item.path) if item.path else None
    shell_only = False

    for file in item.item_files:
        fp = file.file_path

        # Ignore .platform metadata file
        if fp.name == ".platform":
            continue

        # Use the path relative to the environment root, or full path if root is None
        try:
            rel_path = fp.relative_to(root) if root else fp
        except Exception:
            rel_path = fp

        path_parts = [p.lower() for p in rel_path.parts]
        # Detect Setting/Sparkcompute.yml
        if len(path_parts) >= 2 and path_parts[0] == "setting" and path_parts[1] == "sparkcompute.yml":
            shell_only = True
            # Continue checking for other non-.platform files
            continue

        # Any other files -> not shell-only
        return False

    return shell_only


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
        if (
            # Check exclude regex
            (
                not fabric_workspace_obj.publish_item_name_exclude_regex
                or not re.search(fabric_workspace_obj.publish_item_name_exclude_regex, k)
            )
            # Check items_to_include list
            and (
                not fabric_workspace_obj.items_to_include or k + ".Environment" in fabric_workspace_obj.items_to_include
            )
        )
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
            if item_name in filtered_environments:
                if item_state == "running":
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


def _publish_environment_metadata(fabric_workspace_obj: FabricWorkspace, item_name: str) -> None:
    """
    Updates compute settings and publishes compute settings and libraries for a given environment item.

    This process involves two steps:
    1. Check for ongoing publish.
    2. Updating the compute settings.
    3. Publish the updated settings and libraries.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_name: Name of the environment item whose compute settings are to be published.
        is_excluded: Flag indicating if Sparkcompute.yml was excluded from definition deployment.
    """
    item_type = "Environment"
    item_path = fabric_workspace_obj.repository_items[item_type][item_name].path
    item_guid = fabric_workspace_obj.repository_items[item_type][item_name].guid

    # Update compute settings
    _update_compute_settings(fabric_workspace_obj, item_path, item_guid, item_name)

    # Publish updated settings - compute settings and libraries
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/publish?beta=False",
    )
    logger.info(f"{constants.INDENT}Publish Submitted")


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
    from fabric_cicd._parameter._utils import process_environment_key

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
                    replace_value = process_environment_key(fabric_workspace_obj, key["replace_value"])
                    input_name = key.get("item_name")
                    if instance_pool_id == pool_id and (input_name == item_name or not input_name):
                        # replace any found references with specified environment value
                        yaml_body["instancePool"] = replace_value[fabric_workspace_obj.environment]
                        del yaml_body["instance_pool_id"]

        yaml_body = _convert_environment_compute_to_camel(fabric_workspace_obj, yaml_body)

        # Update compute settings
        # https://learn.microsoft.com/en-us/rest/api/fabric/environment/staging/update-spark-compute
        fabric_workspace_obj.endpoint.invoke(
            method="PATCH",
            url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/sparkcompute?beta=False",
            body=yaml_body,
        )
        logger.info(f"{constants.INDENT}Updated Spark Settings")


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
            # Convert spark_conf dict to sparkProperties list of {key, value} objects
            new_key = "sparkProperties"
            # Ensure value is treated as a mapping
            if isinstance(value, dict):
                value = [{"key": k, "value": v} for k, v in value.items()]
        else:
            # Convert the key to camelCase
            key_components = key.split("_")
            new_key = key_components[0] + "".join(x.title() for x in key_components[1:])

        # Recursively update dictionary values if they are dictionaries
        if isinstance(value, dict):
            value = _convert_environment_compute_to_camel(fabric_workspace_obj, value)

        new_input_dict[new_key] = value

    return new_input_dict
