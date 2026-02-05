# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for publishing and unpublishing Fabric workspace items."""

import logging
from typing import Optional

import dpath
from azure.core.credentials import TokenCredential

import fabric_cicd._items as items
from fabric_cicd import constants
from fabric_cicd._common._config_utils import (
    apply_config_overrides,
    extract_publish_settings,
    extract_unpublish_settings,
    extract_workspace_settings,
    load_config_file,
)
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InputError
from fabric_cicd._common._logging import log_header
from fabric_cicd._common._validate_input import (
    validate_environment,
    validate_fabric_workspace_obj,
    validate_folder_path_exclude_regex,
    validate_items_to_include,
    validate_shortcut_exclude_regex,
)
from fabric_cicd.constants import FeatureFlag, ItemType
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_all_items(
    fabric_workspace_obj: FabricWorkspace,
    item_name_exclude_regex: Optional[str] = None,
    folder_path_exclude_regex: Optional[str] = None,
    items_to_include: Optional[list[str]] = None,
    shortcut_exclude_regex: Optional[str] = None,
) -> Optional[dict]:
    """
    Publishes all items defined in the `item_type_in_scope` list of the given FabricWorkspace object.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being published.
        folder_path_exclude_regex: Regex pattern to exclude items based on their folder path.
        items_to_include: List of items in the format "item_name.item_type" that should be published.
        shortcut_exclude_regex: Regex pattern to exclude specific shortcuts from being published in lakehouses.

    Returns:
        Dict containing all API responses if the "enable_response_collection" feature flag is enabled and responses were collected, otherwise None.

    folder_path_exclude_regex:
        This is an experimental feature in fabric-cicd. Use at your own risk as selective deployments are
        not recommended due to item dependencies. To enable this feature, see How To -> Optional Features
        for information on which flags to enable.

    items_to_include:
        This is an experimental feature in fabric-cicd. Use at your own risk as selective deployments are
        not recommended due to item dependencies. To enable this feature, see How To -> Optional Features
        for information on which flags to enable.

    shortcut_exclude_regex:
        This is an experimental feature in fabric-cicd. Use at your own risk as selective shortcut deployments
        may result in missing data dependencies. To enable this feature, see How To -> Optional Features
        for information on which flags to enable.

    Examples:
        Basic usage
        >>> from fabric_cicd import FabricWorkspace, publish_all_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> publish_all_items(workspace)

        With regex name exclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> exclude_regex = ".*_do_not_publish"
        >>> publish_all_items(workspace, item_name_exclude_regex=exclude_regex)

        With folder exclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, append_feature_flag
        >>> append_feature_flag("enable_experimental_features")
        >>> append_feature_flag("enable_exclude_folder")
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> folder_exclude_regex = "^legacy/"
        >>> publish_all_items(workspace, folder_path_exclude_regex=folder_exclude_regex)

        With items to include
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, append_feature_flag
        >>> append_feature_flag("enable_experimental_features")
        >>> append_feature_flag("enable_items_to_include")
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> items_to_include = ["Hello World.Notebook", "Hello.Environment"]
        >>> publish_all_items(workspace, items_to_include=items_to_include)

        With shortcut exclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, append_feature_flag
        >>> append_feature_flag("enable_experimental_features")
        >>> append_feature_flag("enable_shortcut_exclude")
        >>> append_feature_flag("enable_shortcut_publish")
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Lakehouse"]
        ... )
        >>> shortcut_exclude_regex = "^temp_.*"  # Exclude shortcuts starting with "temp_"
        >>> publish_all_items(workspace, shortcut_exclude_regex=shortcut_exclude_regex)

        With response collection
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, append_feature_flag
        >>> append_feature_flag("enable_response_collection")
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> responses = publish_all_items(workspace)
        >>> # Access all responses
        >>> print(responses)
        >>> # Access individual item responses
        >>> notebook_response = workspace.responses["Notebook"]["Hello World"]
    """
    fabric_workspace_obj = validate_fabric_workspace_obj(fabric_workspace_obj)

    # Initialize response collection if feature flag is enabled
    if FeatureFlag.ENABLE_RESPONSE_COLLECTION.value in constants.FEATURE_FLAG:
        fabric_workspace_obj.responses = {}

    # Check if workspace has assigned capacity, if not, exit
    has_assigned_capacity = None

    response_state = fabric_workspace_obj.endpoint.invoke(
        method="GET", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}"
    )

    has_assigned_capacity = dpath.get(response_state, "body/capacityId", default=None)

    if not has_assigned_capacity and not set(fabric_workspace_obj.item_type_in_scope).issubset(
        set(constants.NO_ASSIGNED_CAPACITY_REQUIRED)
    ):
        msg = f"Workspace {fabric_workspace_obj.workspace_id} does not have an assigned capacity. Please assign a capacity before publishing items."
        raise FailedPublishedItemStatusError(msg, logger)

    if FeatureFlag.DISABLE_WORKSPACE_FOLDER_PUBLISH.value not in constants.FEATURE_FLAG:
        fabric_workspace_obj._refresh_deployed_folders()
        fabric_workspace_obj._refresh_repository_folders()
        fabric_workspace_obj._publish_folders()

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_repository_items()

    if item_name_exclude_regex:
        logger.warning(
            "Using item_name_exclude_regex is risky as it can prevent needed dependencies from being deployed.  Use at your own risk."
        )
        fabric_workspace_obj.publish_item_name_exclude_regex = item_name_exclude_regex

    if folder_path_exclude_regex:
        validate_folder_path_exclude_regex(folder_path_exclude_regex)
        fabric_workspace_obj.publish_folder_path_exclude_regex = folder_path_exclude_regex

    if items_to_include:
        validate_items_to_include(items_to_include, operation=constants.OperationType.PUBLISH)
        fabric_workspace_obj.items_to_include = items_to_include

    if shortcut_exclude_regex:
        validate_shortcut_exclude_regex(shortcut_exclude_regex)
        fabric_workspace_obj.shortcut_exclude_regex = shortcut_exclude_regex

    # Publish items in the defined order synchronously
    total_item_types = len(constants.SERIAL_ITEM_PUBLISH_ORDER)
    publishers_with_async_check: list[items.ItemPublisher] = []
    for order_num, item_type in items.ItemPublisher.get_item_types_to_publish(fabric_workspace_obj):
        log_header(f"Publishing Item {order_num}/{total_item_types}: {item_type.value}")
        publisher = items.ItemPublisher.create(item_type, fabric_workspace_obj)
        publisher.publish_all()
        if publisher.has_async_publish_check:
            publishers_with_async_check.append(publisher)

    # Check asynchronous publish status for relevant item types
    for publisher in publishers_with_async_check:
        log_header(f"Checking {publisher.item_type} Publish State")
        publisher.post_publish_all_check()

    # Return response data if feature flag is enabled and responses were collected
    return (
        fabric_workspace_obj.responses
        if FeatureFlag.ENABLE_RESPONSE_COLLECTION.value in constants.FEATURE_FLAG and fabric_workspace_obj.responses
        else None
    )


def unpublish_all_orphan_items(
    fabric_workspace_obj: FabricWorkspace,
    item_name_exclude_regex: str = "^$",
    items_to_include: Optional[list[str]] = None,
) -> None:
    """
    Unpublishes all orphaned items not present in the repository except for those matching the exclude regex.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being unpublished. Default is '^$' which will exclude nothing.
        items_to_include: List of items in the format "item_name.item_type" that should be unpublished.

    items_to_include:
        This is an experimental feature in fabric-cicd. Use at your own risk as selective unpublishing is not recommended due to item dependencies.
        To enable this feature, see How To -> Optional Features for information on which flags to enable.

    Examples:
        Basic usage
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> publish_all_items(workspace)
        >>> unpublish_orphaned_items(workspace)

        With regex name exclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> publish_all_items(workspace)
        >>> exclude_regex = ".*_do_not_delete"
        >>> unpublish_orphaned_items(workspace, item_name_exclude_regex=exclude_regex)

        With items to include
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, append_feature_flag
        >>> append_feature_flag("enable_experimental_features")
        >>> append_feature_flag("enable_items_to_include")
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> publish_all_items(workspace)
        >>> items_to_include = ["Hello World.Notebook", "Run Hello World.DataPipeline"]
        >>> unpublish_orphaned_items(workspace, items_to_include=items_to_include)
    """
    fabric_workspace_obj = validate_fabric_workspace_obj(fabric_workspace_obj)
    validate_items_to_include(items_to_include, operation=constants.OperationType.UNPUBLISH)

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_repository_items()
    log_header(logger, "Unpublishing Orphaned Items")

    # Build unpublish order based on reversed publish order, scope, and feature flags
    for item_type in items.ItemPublisher.get_item_types_to_unpublish(fabric_workspace_obj):
        to_delete_list = items.ItemPublisher.get_orphaned_items(
            fabric_workspace_obj,
            item_type,
            item_name_exclude_regex=item_name_exclude_regex if not items_to_include else None,
            items_to_include=items_to_include,
        )

        if items_to_include and to_delete_list:
            logger.debug(f"Items to include for unpublishing ({item_type}): {to_delete_list}")

        publisher = items.ItemPublisher.create(ItemType(item_type), fabric_workspace_obj)
        if to_delete_list and publisher.has_dependency_tracking:
            to_delete_list = publisher.get_unpublish_order(to_delete_list)

        for item_name in to_delete_list:
            fabric_workspace_obj._unpublish_item(item_name=item_name, item_type=item_type)

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_deployed_folders()
    if FeatureFlag.DISABLE_WORKSPACE_FOLDER_PUBLISH.value not in constants.FEATURE_FLAG:
        fabric_workspace_obj._unpublish_folders()


def deploy_with_config(
    config_file_path: str,
    environment: str = "N/A",
    token_credential: Optional[TokenCredential] = None,
    config_override: Optional[dict] = None,
) -> None:
    """
    Deploy items using YAML configuration file with environment-specific settings.
    This function provides a simplified deployment interface that loads configuration
    from a YAML file and executes deployment operations based on environment-specific
    settings. It constructs the necessary FabricWorkspace object internally
    and handles publish/unpublish operations according to the configuration.

    Args:
        config_file_path: Path to the YAML configuration file as a string.
        environment: Environment name to use for deployment (e.g., 'dev', 'test', 'prod'), if missing defaults to 'N/A'.
        token_credential: Optional Azure token credential for authentication.
        config_override: Optional dictionary to override specific configuration values.

    Raises:
        InputError: If configuration file is invalid or environment not found.
        FileNotFoundError: If configuration file doesn't exist.

    Examples:
        Basic usage
        >>> from fabric_cicd import deploy_with_config
        >>> deploy_with_config(
        ...     config_file_path="workspace/config.yml",
        ...     environment="prod"
        ... )

        With custom authentication
        >>> from fabric_cicd import deploy_with_config
        >>> from azure.identity import ClientSecretCredential
        >>> credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        >>> deploy_with_config(
        ...     config_file_path="workspace/config.yml",
        ...     environment="prod",
        ...     token_credential=credential
        ... )

        With override configuration
        >>> from fabric_cicd import deploy_with_config
        >>> from azure.identity import ClientSecretCredential
        >>> credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        >>> deploy_with_config(
        ...     config_file_path="workspace/config.yml",
        ...     environment="prod",
        ...     config_override={
        ...         "core": {
        ...             "item_types_in_scope": ["Notebook"]
        ...         },
        ...         "publish": {
        ...             "skip": {
        ...                 "prod": False
        ...             }
        ...         }
        ...     }
        ... )
    """
    # Experimental feature flags required to enable
    if (
        FeatureFlag.ENABLE_EXPERIMENTAL_FEATURES.value not in constants.FEATURE_FLAG
        or FeatureFlag.ENABLE_CONFIG_DEPLOY.value not in constants.FEATURE_FLAG
    ):
        msg = "Config file-based deployment is currently an experimental feature. Both 'enable_experimental_features' and 'enable_config_deploy' feature flags must be set."
        raise InputError(msg, logger)

    log_header(logger, "Config-Based Deployment")
    logger.info(f"Loading configuration from {config_file_path} for environment '{environment}'")

    # Validate environment
    environment = validate_environment(environment)

    # Load and validate configuration file
    config = load_config_file(config_file_path, environment, config_override)

    # Extract environment-specific settings
    workspace_settings = extract_workspace_settings(config, environment)
    publish_settings = extract_publish_settings(config, environment)
    unpublish_settings = extract_unpublish_settings(config, environment)

    # Apply feature flags and constants if specified
    apply_config_overrides(config, environment)

    # Create FabricWorkspace object with extracted settings
    workspace = FabricWorkspace(
        repository_directory=workspace_settings["repository_directory"],
        item_type_in_scope=workspace_settings.get("item_types_in_scope"),
        environment=environment,
        workspace_id=workspace_settings.get("workspace_id"),
        workspace_name=workspace_settings.get("workspace_name"),
        token_credential=token_credential,
        parameter_file_path=workspace_settings.get("parameter_file_path"),
    )
    # Execute deployment operations based on skip settings
    if not publish_settings.get("skip", False):
        publish_all_items(
            workspace,
            item_name_exclude_regex=publish_settings.get("exclude_regex"),
            folder_path_exclude_regex=publish_settings.get("folder_exclude_regex"),
            items_to_include=publish_settings.get("items_to_include"),
            shortcut_exclude_regex=publish_settings.get("shortcut_exclude_regex"),
        )
    else:
        logger.info(f"Skipping publish operation for environment '{environment}'")

    if not unpublish_settings.get("skip", False):
        unpublish_all_orphan_items(
            workspace,
            item_name_exclude_regex=unpublish_settings.get("exclude_regex", "^$"),
            items_to_include=unpublish_settings.get("items_to_include"),
        )
    else:
        logger.info(f"Skipping unpublish operation for environment '{environment}'")

    logger.info("Config-based deployment completed successfully")
