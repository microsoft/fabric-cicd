# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Environment item."""

import logging
import re

import dpath
import yaml

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._fabric_endpoint import handle_retry
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType

logger = logging.getLogger(__name__)


def _process_environment_file(fabric_workspace_obj: FabricWorkspace, item: Item, file_obj: File) -> str:
    """
    Process an Environment item file before it is included in the item definition payload.

    For ``Setting/Sparkcompute.yml`` this performs ``instance_pool_id`` replacement
    using the ``spark_pool`` parameter configuration so that the correct pool
    reference is embedded directly in the YAML sent to the Fabric Items API.

    All other files are returned unchanged.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item: The Item object representing the Environment.
        file_obj: The File object being processed.

    Returns:
        The (possibly modified) file contents as a string.
    """
    if not (file_obj.file_path.name == "Sparkcompute.yml" and file_obj.file_path.parent.name == "Setting"):
        return file_obj.contents

    contents = file_obj.contents

    if not "instance_pool_id" in contents:
        return contents

    yaml_body = yaml.safe_load(contents)
    if not isinstance(yaml_body, dict):
        return contents

    if "instance_pool_id" in yaml_body:
        yaml_body = _replace_instance_pool_id(fabric_workspace_obj, yaml_body, item.name)

    return yaml.dump(yaml_body, default_flow_style=False, sort_keys=False)


def _replace_instance_pool_id(fabric_workspace_obj: FabricWorkspace, yaml_body: dict, item_name: str) -> dict:
    """
    Replace ``instance_pool_id`` in parsed Sparkcompute YAML with a resolved pool GUID.

    This function reads ``spark_pool`` parameter mappings from
    ``fabric_workspace_obj.environment_parameter`` and finds the entry whose
    ``instance_pool_id`` matches the current YAML value. If an ``item_name`` is
    provided in the mapping, it must match the current Environment item name;
    otherwise, the mapping applies globally.

    The mapped target pool ``name`` and ``type`` are then resolved against the
    workspace custom pool list returned by the Fabric API, and the resolved pool
    ``id`` is written back to ``yaml_body["instance_pool_id"]``.

    Args:
        fabric_workspace_obj: Workspace context containing environment, parameters,
            and endpoint configuration.
        yaml_body: Parsed contents of ``Setting/Sparkcompute.yml``.
        item_name: Environment item name used for optional per-item mapping filters.

    Returns:
        The YAML dictionary, updated if a matching mapping is found; otherwise unchanged.
    """
    from fabric_cicd._parameter._utils import process_environment_key

    # https://learn.microsoft.com/en-us/rest/api/fabric/spark/workspace-custom-pools/list-workspace-custom-pools
    response = fabric_workspace_obj.endpoint.invoke(
        method="GET",
        url=f"{fabric_workspace_obj.base_api_url}/spark/pools",
    )
    pools = response["body"]["value"] or []

    pool_id = yaml_body["instance_pool_id"]
    if "spark_pool" in fabric_workspace_obj.environment_parameter:
        parameter_dict = fabric_workspace_obj.environment_parameter["spark_pool"]
        for key in parameter_dict:
            instance_pool_id = key["instance_pool_id"]
            replace_value = process_environment_key(fabric_workspace_obj.environment, key["replace_value"])
            input_name = key.get("item_name")
            if instance_pool_id == pool_id and (input_name == item_name or not input_name):
                pool_config = replace_value[fabric_workspace_obj.environment]
                resolved_id = _resolve_pool_id(
                    pools,
                    pool_name=pool_config["name"],
                    pool_type=pool_config["type"],
                )
                yaml_body["instance_pool_id"] = resolved_id
                break

    return yaml_body


def _resolve_pool_id(pools: list[dict], pool_name: str, pool_type: str) -> str:
    """
    Resolve a workspace custom Spark pool ID by pool ``name`` and ``type``.

    Args:
        pools: Pool objects from ``GET /spark/pools`` (expected to include
            ``name``, ``type``, and ``id`` fields).
        pool_name: Target pool display name.
        pool_type: Target pool type (for example, ``"Capacity"`` or ``"Workspace"``).

    Returns:
        The matching pool GUID.

    Raises:
        Exception: If no pool exists with the specified ``name`` and ``type``.
    """
    for pool in pools:
        if pool["name"] == pool_name and pool["type"] == pool_type:
            return pool["id"]

    msg = (
        f"Could not resolve custom Spark pool: name='{pool_name}', type='{pool_type}'. "
        f"No matching pool found in the target workspace."
    )
    raise InputError(msg, logger)


def _check_environment_publish_state(fabric_workspace_obj: FabricWorkspace, initial_check: bool = False) -> None:
    """
    Checks the publish state of environments after deployment.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        initial_check: Flag to ignore publish failures on initial check.
    """
    ongoing_publish = True
    iteration = 1

    environments = fabric_workspace_obj.repository_items.get(ItemType.ENVIRONMENT.value, {})
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
        completed = []
        running = []
        failed = []

        response_state = fabric_workspace_obj.endpoint.invoke(
            method="GET", url=f"{fabric_workspace_obj.base_api_url}/environments/"
        )

        for item in response_state["body"]["value"]:
            item_name = item["displayName"]
            item_state = dpath.get(item, "properties/publishDetails/state", default="").lower()
            if item_name in filtered_environments:
                if item_state == "running":
                    running.append(item_name)
                    ongoing_publish = True
                elif item_state == "success":
                    completed.append(item_name)
                elif item_state in ["failed", "cancelled"]:
                    failed.append(item_name)
                    if not initial_check:
                        msg = f"Publish {item_state} for Environment '{item_name}'"
                        raise Exception(msg)
        logger.debug(
            f"Environment publish states - Running: {running}, Succeeded: {completed}, Failed/Cancelled: {failed}"
        )
        if ongoing_publish:
            handle_retry(
                attempt=iteration,
                base_delay=5,
                response_retry_after=120,
                prepend_message=f"{constants.INDENT}Operation in progress.",
            )
            iteration += 1

    if not initial_check:
        logger.info(f"{constants.INDENT}Published: {completed}")


def _submit_environment_publish(fabric_workspace_obj: FabricWorkspace, item_name: str) -> None:
    """
    Submit a publish request for an Environment item.

    Triggers the asynchronous publish of the environment's staged settings and
    libraries. The publish state is monitored separately by the async publish
    check hooks.

    Args:
        fabric_workspace_obj: The FabricWorkspace object.
        item_name: Name of the environment item to publish.
    """
    item_type = ItemType.ENVIRONMENT.value
    item_guid = fabric_workspace_obj.repository_items[item_type][item_name].guid

    # Publish updated settings - compute settings and libraries (long-running operation)
    # https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment
    fabric_workspace_obj.endpoint.invoke(
        method="POST",
        url=f"{fabric_workspace_obj.base_api_url}/environments/{item_guid}/staging/publish?beta=False",
        poll_long_running=False,
    )
    logger.info(f"{constants.INDENT}Publish Submitted for Environment '{item_name}'")


class EnvironmentPublisher(ItemPublisher):
    """Publisher for Environment items."""

    item_type = ItemType.ENVIRONMENT.value
    has_async_publish_check = True

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Environment item."""
        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            func_process_file=_process_environment_file,
            skip_publish_logging=True,
        )
        if item.skip_publish:
            return
        _submit_environment_publish(self.fabric_workspace_obj, item_name)

    def pre_publish_all(self) -> None:
        """Check environment publish state before publishing."""
        _check_environment_publish_state(self.fabric_workspace_obj, True)

    def post_publish_all_check(self) -> None:
        """Check environment publish state after all environments have been published."""
        _check_environment_publish_state(self.fabric_workspace_obj, False)
