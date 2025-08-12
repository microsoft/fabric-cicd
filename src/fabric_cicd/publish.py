# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for publishing and unpublishing Fabric workspace items."""

import logging
from pathlib import Path
from typing import Optional

import dpath.util as dpath
import yaml
from azure.core.credentials import TokenCredential

import fabric_cicd._items as items
from fabric_cicd import constants
from fabric_cicd._common._check_utils import check_regex
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InputError
from fabric_cicd._common._logging import print_header
from fabric_cicd._common._validate_input import (
    validate_fabric_workspace_obj,
)
from fabric_cicd.fabric_workspace import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_all_items(
    fabric_workspace_obj: FabricWorkspace,
    item_name_exclude_regex: Optional[str] = None,
    items_to_include: Optional[list[str]] = None,
) -> None:
    """
    Publishes all items defined in the `item_type_in_scope` list of the given FabricWorkspace object.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being published.
        items_to_include: List of items in the format "item_name.item_type" that should be published.


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

        With items to include
        >>> from fabric_cicd import FabricWorkspace, publish_all_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> items_to_include = ["Hello World.Notebook", "Hello.Environment"]
        >>> publish_all_items(workspace, items_to_include=items_to_include)
    """
    fabric_workspace_obj = validate_fabric_workspace_obj(fabric_workspace_obj)

    # check if workspace has assigned capacity, if not, exit
    has_assigned_capacity = None

    response_state = fabric_workspace_obj.endpoint.invoke(
        method="GET", url=f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}"
    )

    has_assigned_capacity = dpath.get(response_state, "body/capacityId", default=None)

    if (
        not has_assigned_capacity
        and fabric_workspace_obj.item_type_in_scope not in constants.NO_ASSIGNED_CAPACITY_REQUIRED
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

    if items_to_include:
        if "enable_experimental_features" not in constants.FEATURE_FLAG:
            msg = "A list of items to include was provided, but the 'enable_experimental_features' feature flag is not set."
            raise InputError(msg, logger)
        if "enable_items_to_include" not in constants.FEATURE_FLAG:
            msg = "Experimental features are enabled but the 'enable_items_to_include' feature flag is not set."
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
    if _should_publish_item_type("Lakehouse"):
        print_header("Publishing Lakehouses")
        items.publish_lakehouses(fabric_workspace_obj)
    if _should_publish_item_type("SQLDatabase"):
        print_header("Publishing SQL Databases")
        items.publish_sqldatabases(fabric_workspace_obj)
    if _should_publish_item_type("MirroredDatabase"):
        print_header("Publishing Mirrored Databases")
        items.publish_mirroreddatabase(fabric_workspace_obj)
    if _should_publish_item_type("Environment"):
        print_header("Publishing Environments")
        items.publish_environments(fabric_workspace_obj)
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
    if _should_publish_item_type("Eventhouse"):
        print_header("Publishing Eventhouses")
        items.publish_eventhouses(fabric_workspace_obj)
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
        logger.warning(
            "Only user authentication is supported for GraphQL API items sourced from SQL Analytics Endpoint"
        )
        items.publish_graphqlapis(fabric_workspace_obj)

    # Check Environment Publish
    if _should_publish_item_type("Environment"):
        print_header("Checking Environment Publish State")
        items.check_environment_publish_state(fabric_workspace_obj)


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
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
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

    regex_pattern = check_regex(item_name_exclude_regex)

    fabric_workspace_obj._refresh_deployed_items()
    fabric_workspace_obj._refresh_repository_items()
    print_header("Unpublishing Orphaned Items")

    if items_to_include:
        if "enable_experimental_features" not in constants.FEATURE_FLAG:
            msg = "A list of items to include was provided, but the 'enable_experimental_features' feature flag is not set."
            raise InputError(msg, logger)
        if "enable_items_to_include" not in constants.FEATURE_FLAG:
            msg = "Experimental features are enabled but the 'enable_items_to_include' feature flag is not set."
            raise InputError(msg, logger)
        logger.warning("Selective unpublish is enabled.")
        logger.warning(
            "Using items_to_include is risky as it can prevent needed dependencies from being unpublished.  Use at your own risk."
        )
        fabric_workspace_obj.items_to_include = items_to_include

    # Lakehouses, SQL Databases, and Warehouses can only be unpublished if their feature flags are set
    unpublish_flag_mapping = {
        "Lakehouse": "enable_lakehouse_unpublish",
        "SQLDatabase": "enable_sqldatabase_unpublish",
        "Warehouse": "enable_warehouse_unpublish",
    }

    # Define order to unpublish items
    unpublish_order = []
    for item_type in [
        "GraphQLApi",
        "DataPipeline",
        "Dataflow",
        "Eventstream",
        "Reflex",
        "KQLDashboard",
        "KQLQueryset",
        "KQLDatabase",
        "Eventhouse",
        "CopyJob",
        "Report",
        "SemanticModel",
        "Notebook",
        "Environment",
        "MirroredDatabase",
        "SQLDatabase",
        "Lakehouse",
        "Warehouse",
        "VariableLibrary",
    ]:
        if item_type in fabric_workspace_obj.item_type_in_scope and item_type in fabric_workspace_obj.deployed_items:
            unpublish_flag = unpublish_flag_mapping.get(item_type)
            # Append item_type if no feature flag is required or the corresponding flag is enabled
            if not unpublish_flag or unpublish_flag in constants.FEATURE_FLAG:
                unpublish_order.append(item_type)

    for item_type in unpublish_order:
        deployed_names = set(fabric_workspace_obj.deployed_items.get(item_type, {}).keys())
        repository_names = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())

        to_delete_set = deployed_names - repository_names
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
    config_file: str,
    environment: str,
    token_credential: Optional[TokenCredential] = None,
) -> None:
    """
    Deploy items using YAML configuration file with environment-specific settings.

    This function provides a simplified deployment interface that loads configuration
    from a YAML file and executes deployment operations based on environment-specific
    skip settings. It constructs the necessary FabricWorkspace object internally
    and handles publish/unpublish operations according to the configuration.

    Args:
        config_file: Path to the YAML configuration file.
        environment: Environment name to use for deployment (e.g., 'dev', 'test', 'prod').
        token_credential: Optional Azure token credential for authentication.

    Raises:
        InputError: If configuration file is invalid or environment not found.
        FileNotFoundError: If configuration file doesn't exist.

    Examples:
        Basic usage
        >>> from fabric_cicd import deploy_with_config
        >>> deploy_with_config(
        ...     config_file="config.yml",
        ...     environment="dev"
        ... )

        With custom authentication
        >>> from fabric_cicd import deploy_with_config
        >>> from azure.identity import ClientSecretCredential
        >>> credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        >>> deploy_with_config(
        ...     config_file="config.yml",
        ...     environment="prod",
        ...     token_credential=credential
        ... )

    Config file structure:
        ```yaml
        core:
          workspace_id:
            dev: 8b6e2c7a-4c1f-4e3a-9b2e-7d8f2e1a6c3b
            test: 2f4b9e8d-1a7c-4d3e-b8e2-5c9f7a2d4e1b
            prod: 7c3e1f8b-2d4a-4b9e-8f2c-1a6c3b7d8e2f
          repository_directory: "sample/workspace"
          item_types_in_scope: [Environment, Notebook, DataPipeline]

        publish:
          exclude_regex: "^DONT_DEPLOY.*"
          skip:
            dev: true
            test: false
            prod: false

        unpublish:
          exclude_regex: "^DEBUG.*"
          skip:
            dev: true
            test: false
            prod: false

        features:
          - enable_shortcut_publish

        constants:
          DEFAULT_API_ROOT_URL: "https://msitapi.fabric.microsoft.com"
        ```
    """
    print_header("Config-Based Deployment")
    logger.info(f"Loading configuration from {config_file} for environment '{environment}'")

    # Load and validate configuration file
    config = _load_config_file(config_file)

    # Extract environment-specific settings
    workspace_settings = _extract_workspace_settings(config, environment)
    publish_settings = _extract_publish_settings(config, environment)
    unpublish_settings = _extract_unpublish_settings(config, environment)

    # Apply feature flags and constants if specified
    _apply_config_overrides(config)

    # Create FabricWorkspace object with extracted settings
    workspace = FabricWorkspace(
        workspace_id=workspace_settings.get("workspace_id"),
        workspace_name=workspace_settings.get("workspace_name"),
        repository_directory=workspace_settings["repository_directory"],
        item_type_in_scope=workspace_settings.get("item_types_in_scope"),
        environment=environment,
        token_credential=token_credential,
    )

    # Execute deployment operations based on skip settings
    if not publish_settings.get("skip", False):
        logger.info("Publishing items from repository")
        publish_all_items(
            workspace,
            item_name_exclude_regex=publish_settings.get("exclude_regex"),
            items_to_include=publish_settings.get("items_to_include"),
        )
    else:
        logger.info(f"Skipping publish operation for environment '{environment}'")

    if not unpublish_settings.get("skip", False):
        logger.info("Unpublishing orphaned items")
        unpublish_all_orphan_items(
            workspace,
            item_name_exclude_regex=unpublish_settings.get("exclude_regex"),
            items_to_include=unpublish_settings.get("items_to_include"),
        )
    else:
        logger.info(f"Skipping unpublish operation for environment '{environment}'")

    logger.info("Config-based deployment completed successfully")


def _load_config_file(config_file: str) -> dict:
    """Load and validate YAML configuration file."""
    config_path = Path(config_file)
    if not config_path.exists():
        error_msg = f"Configuration file not found: {config_file}"
        raise FileNotFoundError(error_msg)

    try:
        with config_path.open(encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML syntax in configuration file: {e}"
        raise InputError(error_msg, logger) from e

    if not isinstance(config, dict):
        error_msg = "Configuration file must contain a YAML dictionary"
        raise InputError(error_msg, logger)

    # Validate required sections
    if "core" not in config:
        error_msg = "Configuration file must contain a 'core' section"
        raise InputError(error_msg, logger)

    return config


def _extract_workspace_settings(config: dict, environment: str) -> dict:
    """Extract workspace-specific settings from config for the given environment."""
    core = config["core"]
    settings = {}

    # Extract workspace ID or name based on environment
    if "workspace_id" in core:
        if isinstance(core["workspace_id"], dict):
            if environment not in core["workspace_id"]:
                error_msg = f"Environment '{environment}' not found in workspace_id mappings"
                raise InputError(error_msg, logger)
            settings["workspace_id"] = core["workspace_id"][environment]
        else:
            settings["workspace_id"] = core["workspace_id"]

    if "workspace" in core:
        if isinstance(core["workspace"], dict):
            if environment not in core["workspace"]:
                error_msg = f"Environment '{environment}' not found in workspace mappings"
                raise InputError(error_msg, logger)
            settings["workspace_name"] = core["workspace"][environment]
        else:
            settings["workspace_name"] = core["workspace"]

    # Validate that either workspace_id or workspace_name is provided
    if "workspace_id" not in settings and "workspace_name" not in settings:
        error_msg = "Configuration must specify either 'workspace_id' or 'workspace' in core section"
        raise InputError(error_msg, logger)

    # Extract other required settings
    if "repository_directory" not in core:
        error_msg = "Configuration must specify 'repository_directory' in core section"
        raise InputError(error_msg, logger)
    settings["repository_directory"] = core["repository_directory"]

    # Optional settings
    if "item_types_in_scope" in core:
        settings["item_types_in_scope"] = core["item_types_in_scope"]

    return settings


def _extract_publish_settings(config: dict, environment: str) -> dict:
    """Extract publish-specific settings from config for the given environment."""
    settings = {}

    if "publish" in config:
        publish_config = config["publish"]

        # Extract exclude regex
        if "exclude_regex" in publish_config:
            settings["exclude_regex"] = publish_config["exclude_regex"]

        # Extract items to include
        if "items_to_include" in publish_config:
            settings["items_to_include"] = publish_config["items_to_include"]

        # Extract environment-specific skip setting
        if "skip" in publish_config:
            if isinstance(publish_config["skip"], dict):
                settings["skip"] = publish_config["skip"].get(environment, False)
            else:
                settings["skip"] = publish_config["skip"]

    return settings


def _extract_unpublish_settings(config: dict, environment: str) -> dict:
    """Extract unpublish-specific settings from config for the given environment."""
    settings = {}

    if "unpublish" in config:
        unpublish_config = config["unpublish"]

        # Extract exclude regex
        if "exclude_regex" in unpublish_config:
            settings["exclude_regex"] = unpublish_config["exclude_regex"]

        # Extract items to include
        if "items_to_include" in unpublish_config:
            settings["items_to_include"] = unpublish_config["items_to_include"]

        # Extract environment-specific skip setting
        if "skip" in unpublish_config:
            if isinstance(unpublish_config["skip"], dict):
                settings["skip"] = unpublish_config["skip"].get(environment, False)
            else:
                settings["skip"] = unpublish_config["skip"]

    return settings


def _apply_config_overrides(config: dict) -> None:
    """Apply feature flags and constants overrides from config."""
    # Apply feature flags
    if "features" in config and isinstance(config["features"], list):
        for feature in config["features"]:
            if isinstance(feature, str):
                constants.FEATURE_FLAG.add(feature)
                logger.info(f"Enabled feature flag: {feature}")

    # Apply constants overrides
    if "constants" in config and isinstance(config["constants"], dict):
        for key, value in config["constants"].items():
            if hasattr(constants, key):
                setattr(constants, key, value)
                logger.info(f"Override constant {key} = {value}")
            else:
                logger.warning(f"Unknown constant '{key}' in configuration, ignoring")
