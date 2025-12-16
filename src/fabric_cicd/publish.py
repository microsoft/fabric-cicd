# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for publishing and unpublishing Fabric workspace items."""

import logging
from typing import Optional

import dpath
from azure.core.credentials import TokenCredential

import fabric_cicd._items as items
from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_regex
from fabric_cicd._common._config_utils import (
    apply_config_overrides,
    extract_publish_settings,
    extract_unpublish_settings,
    extract_workspace_settings,
    load_config_file,
)
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InputError
from fabric_cicd._common._logging import print_header
from fabric_cicd._common._validate_input import (
    validate_environment,
    validate_fabric_workspace_obj,
)
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_all_items(
    fabric_workspace_obj: FabricWorkspace,
    item_name_exclude_regex: Optional[str] = None,
    folder_path_exclude_regex: Optional[str] = None,
    items_to_include: Optional[list[str]] = None,
) -> Optional[dict]:
    """
    Publishes all items defined in the `item_type_in_scope` list of the given FabricWorkspace object.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being published.
        folder_path_exclude_regex: Regex pattern to exclude items based on their folder path.
        items_to_include: List of items in the format "item_name.item_type" that should be published.

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
    if "enable_response_collection" in constants.FEATURE_FLAG:
        fabric_workspace_obj.responses = {}

    # check if workspace has assigned capacity, if not, exit
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

    if "disable_workspace_folder_publish" not in constants.FEATURE_FLAG:
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
        if (
            "enable_experimental_features" not in constants.FEATURE_FLAG
            or "enable_exclude_folder" not in constants.FEATURE_FLAG
        ):
            msg = "Feature flags 'enable_experimental_features' and 'enable_exclude_folder' must be set."
            raise InputError(msg, logger)
        logger.warning("Folder path exclusion is enabled.")
        logger.warning(
            "Using folder_path_exclude_regex is risky as it can prevent needed dependencies from being deployed.  Use at your own risk."
        )
        fabric_workspace_obj.publish_folder_path_exclude_regex = folder_path_exclude_regex

    if items_to_include:
        if (
            "enable_experimental_features" not in constants.FEATURE_FLAG
            or "enable_items_to_include" not in constants.FEATURE_FLAG
        ):
            msg = "Feature flags 'enable_experimental_features' and 'enable_items_to_include' must be set."
            raise InputError(msg, logger)
        logger.warning("Selective deployment is enabled.")
        logger.warning(
            "Using items_to_include is risky as it can prevent needed dependencies from being deployed.  Use at your own risk."
        )
        fabric_workspace_obj.items_to_include = items_to_include

    def _should_publish_item_type(item_type: str) -> bool:
        """Check if an item type should be published based on scope and repository content."""
        return (
            item_type in fabric_workspace_obj.item_type_in_scope and item_type in fabric_workspace_obj.repository_items
        )

    if _should_publish_item_type("VariableLibrary"):
        print_header("Publishing Variable Libraries")
        items.publish_variablelibraries(fabric_workspace_obj)
    if _should_publish_item_type("Warehouse"):
        print_header("Publishing Warehouses")
        items.publish_warehouses(fabric_workspace_obj)
    if _should_publish_item_type("MirroredDatabase"):
        print_header("Publishing Mirrored Databases")
        items.publish_mirroreddatabase(fabric_workspace_obj)
    if _should_publish_item_type("Lakehouse"):
        print_header("Publishing Lakehouses")
        items.publish_lakehouses(fabric_workspace_obj)
    if _should_publish_item_type("SQLDatabase"):
        print_header("Publishing SQL Databases")
        items.publish_sqldatabases(fabric_workspace_obj)
    if _should_publish_item_type("Environment"):
        print_header("Publishing Environments")
        items.publish_environments(fabric_workspace_obj)
    if _should_publish_item_type("UserDataFunction"):
        print_header("Publishing User Data Functions")
        items.publish_userdatafunctions(fabric_workspace_obj)
    if _should_publish_item_type("Eventhouse"):
        print_header("Publishing Eventhouses")
        items.publish_eventhouses(fabric_workspace_obj)
    if _should_publish_item_type("Notebook"):
        print_header("Publishing Notebooks")
        items.publish_notebooks(fabric_workspace_obj)
    if _should_publish_item_type("SemanticModel"):
        print_header("Publishing Semantic Models")
        items.publish_semanticmodels(fabric_workspace_obj)
    if _should_publish_item_type("Report"):
        print_header("Publishing Reports")
        items.publish_reports(fabric_workspace_obj)
    if _should_publish_item_type("CopyJob"):
        print_header("Publishing Copy Jobs")
        items.publish_copyjobs(fabric_workspace_obj)
    if _should_publish_item_type("KQLDatabase"):
        print_header("Publishing KQL Databases")
        items.publish_kqldatabases(fabric_workspace_obj)
    if _should_publish_item_type("KQLQueryset"):
        print_header("Publishing KQL Querysets")
        items.publish_kqlquerysets(fabric_workspace_obj)
    if _should_publish_item_type("Reflex"):
        print_header("Publishing Activators")
        items.publish_activators(fabric_workspace_obj)
    if _should_publish_item_type("Eventstream"):
        print_header("Publishing Eventstreams")
        items.publish_eventstreams(fabric_workspace_obj)
    if _should_publish_item_type("KQLDashboard"):
        print_header("Publishing KQL Dashboards")
        items.publish_kqldashboard(fabric_workspace_obj)
    if _should_publish_item_type("Dataflow"):
        print_header("Publishing Dataflows")
        items.publish_dataflows(fabric_workspace_obj)
    if _should_publish_item_type("DataPipeline"):
        print_header("Publishing Data Pipelines")
        items.publish_datapipelines(fabric_workspace_obj)
    if _should_publish_item_type("GraphQLApi"):
        print_header("Publishing GraphQL APIs")
        items.publish_graphqlapis(fabric_workspace_obj)
    if _should_publish_item_type("ApacheAirflowJob"):
        print_header("Publishing Apache Airflow Jobs")
        items.publish_apacheairflowjobs(fabric_workspace_obj)
    if _should_publish_item_type("MountedDataFactory"):
        print_header("Publishing Mounted Data Factories")
        items.publish_mounteddatafactories(fabric_workspace_obj)
    if _should_publish_item_type("OrgApp"):
        print_header("Publishing Org Apps")
        items.publish_orgapps(fabric_workspace_obj)
    if _should_publish_item_type("DataAgent"):
        print_header("Publishing Data Agents")
        items.publish_dataagents(fabric_workspace_obj)
    if _should_publish_item_type("MLExperiment"):
        print_header("Publishing ML Experiments")
        items.publish_mlexperiments(fabric_workspace_obj)

    # Check Environment Publish
    if _should_publish_item_type("Environment"):
        print_header("Checking Environment Publish State")
        items.check_environment_publish_state(fabric_workspace_obj)

    # Return response data if feature flag is enabled and responses were collected
    return (
        fabric_workspace_obj.responses
        if "enable_response_collection" in constants.FEATURE_FLAG and fabric_workspace_obj.responses
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

    is_items_to_include_list = False
    regex_pattern = check_regex(item_name_exclude_regex)

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_repository_items()
    print_header("Unpublishing Orphaned Items")

    if items_to_include:
        if (
            "enable_experimental_features" not in constants.FEATURE_FLAG
            or "enable_items_to_include" not in constants.FEATURE_FLAG
        ):
            msg = "Feature flags 'enable_experimental_features' and 'enable_items_to_include' must be set."
            raise InputError(msg, logger)
        logger.warning("Selective unpublish is enabled.")
        logger.warning(
            "Using items_to_include is risky as it can prevent needed dependencies from being unpublished.  Use at your own risk."
        )
        is_items_to_include_list = True

    # Lakehouses, SQL Databases, and Warehouses can only be unpublished if their feature flags are set
    unpublish_flag_mapping = {
        "Lakehouse": "enable_lakehouse_unpublish",
        "SQLDatabase": "enable_sqldatabase_unpublish",
        "Warehouse": "enable_warehouse_unpublish",
        "Eventhouse": "enable_eventhouse_unpublish",
        "KQLDatabase": "enable_kqldatabase_unpublish",
    }

    # Define order to unpublish items
    unpublish_order = []
    for item_type in [
        "MLExperiment",
        "DataAgent",
        "OrgApp",
        "MountedDataFactory",
        "ApacheAirflowJob",
        "GraphQLApi",
        "DataPipeline",
        "Dataflow",
        "KQLDashboard",
        "Eventstream",
        "Reflex",
        "KQLQueryset",
        "KQLDatabase",
        "CopyJob",
        "Report",
        "SemanticModel",
        "Notebook",
        "Eventhouse",
        "UserDataFunction",
        "Environment",
        "SQLDatabase",
        "Lakehouse",
        "MirroredDatabase",
        "Warehouse",
        "VariableLibrary",
    ]:
        if item_type in fabric_workspace_obj.item_type_in_scope and item_type in fabric_workspace_obj.deployed_items:
            unpublish_flag = unpublish_flag_mapping.get(item_type)
            # Append item_type if no feature flag is required or the corresponding flag is enabled
            if not unpublish_flag or unpublish_flag in constants.FEATURE_FLAG:
                unpublish_order.append(item_type)
            elif unpublish_flag and unpublish_flag not in constants.FEATURE_FLAG:
                # Log warning when unpublish is skipped due to missing feature flag
                logger.warning(
                    f"Skipping unpublish for {item_type} items because the '{unpublish_flag}' feature flag is not enabled."
                )

    for item_type in unpublish_order:
        deployed_names = set(fabric_workspace_obj.deployed_items.get(item_type, {}).keys())
        repository_names = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())

        to_delete_set = deployed_names - repository_names

        if is_items_to_include_list:
            to_delete_list = [name for name in to_delete_set if f"{name}.{item_type}" in items_to_include]
            logger.debug(f"Items to include for unpublishing ({item_type}): {to_delete_list}")
        else:
            to_delete_list = [name for name in to_delete_set if not regex_pattern.match(name)]

        if item_type == "DataPipeline":
            find_referenced_items_func = items.find_referenced_datapipelines

            # Determine order to delete w/o dependencies
            to_delete_list = items.set_unpublish_order(
                fabric_workspace_obj, item_type, to_delete_list, find_referenced_items_func
            )

        for item_name in to_delete_list:
            fabric_workspace_obj._unpublish_item(item_name=item_name, item_type=item_type)

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_deployed_folders()
    if "disable_workspace_folder_publish" not in constants.FEATURE_FLAG:
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
        "enable_experimental_features" not in constants.FEATURE_FLAG
        or "enable_config_deploy" not in constants.FEATURE_FLAG
    ):
        msg = "Config file-based deployment is currently an experimental feature. Both 'enable_experimental_features' and 'enable_config_deploy' feature flags must be set."
        raise InputError(msg, logger)

    print_header("Config-Based Deployment")
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
