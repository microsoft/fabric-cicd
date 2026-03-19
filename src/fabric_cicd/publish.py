# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Module for publishing and unpublishing Fabric workspace items."""

import json
import logging
import subprocess
from pathlib import Path
from typing import Optional

import dpath
from azure.core.credentials import TokenCredential

import fabric_cicd._items as items
from fabric_cicd import constants
from fabric_cicd._common._config_utils import (
    config_overrides_scope,
    extract_publish_settings,
    extract_unpublish_settings,
    extract_workspace_settings,
    load_config_file,
)
from fabric_cicd._common._deployment_result import DeploymentResult, DeploymentStatus
from fabric_cicd._common._exceptions import FailedPublishedItemStatusError, InputError
from fabric_cicd._common._logging import log_header
from fabric_cicd._common._validate_input import (
    validate_environment,
    validate_fabric_workspace_obj,
    validate_folder_path_exclude_regex,
    validate_folder_path_to_include,
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
    folder_path_to_include: Optional[list[str]] = None,
    items_to_include: Optional[list[str]] = None,
    shortcut_exclude_regex: Optional[str] = None,
) -> Optional[dict]:
    """
    Publishes all items defined in the `item_type_in_scope` list of the given FabricWorkspace object.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_name_exclude_regex: Regex pattern to exclude specific items from being published.
        folder_path_exclude_regex: Regex pattern matched against folder paths (e.g., "/folder_name") to exclude folders and their items from being published.
        folder_path_to_include: List of folder paths in the format "/folder_name"; only the specified folders and their items will be published.
        items_to_include: List of items in the format "item_name.item_type" that should be published.
        shortcut_exclude_regex: Regex pattern to exclude specific shortcuts from being published in lakehouses.

    Returns:
        Dict containing all API responses if the "enable_response_collection" feature flag is enabled and responses were collected, otherwise None.

    folder_path_exclude_regex:
        This is an experimental feature in fabric-cicd. Use at your own risk as selective deployments are
        not recommended due to item dependencies. Cannot be used together with ``folder_path_to_include``
        for the same environment. To enable this feature, see How To -> Optional Features for information
        on which flags to enable.

    folder_path_to_include:
        This is an experimental feature in fabric-cicd. Use at your own risk as selective deployments are
        not recommended due to item dependencies. Cannot be used together with ``folder_path_exclude_regex``
        for the same environment. To enable this feature, see How To -> Optional Features for information
        on which flags to enable.

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
        >>> folder_exclude_regex = "^/legacy"
        >>> publish_all_items(workspace, folder_path_exclude_regex=folder_exclude_regex)

        With folder inclusion
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, append_feature_flag
        >>> append_feature_flag("enable_experimental_features")
        >>> append_feature_flag("enable_include_folder")
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Environment", "Notebook", "DataPipeline"]
        ... )
        >>> folder_path_to_include = ["/subfolder"]
        >>> publish_all_items(workspace, folder_path_to_include=folder_path_to_include)

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

        With get_changed_items (deploy only git-changed items)
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, get_changed_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Notebook", "DataPipeline"]
        ... )
        >>> changed = get_changed_items(workspace.repository_directory)
        >>> if changed:
        ...     publish_all_items(workspace, items_to_include=changed)
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
        if folder_path_exclude_regex is not None and folder_path_to_include is not None:
            msg = "Cannot use both 'folder_path_exclude_regex' and 'folder_path_to_include' simultaneously. Choose one filtering strategy."
            raise InputError(msg, logger)

        if folder_path_exclude_regex is not None:
            validate_folder_path_exclude_regex(folder_path_exclude_regex)
            fabric_workspace_obj.publish_folder_path_exclude_regex = folder_path_exclude_regex

        if folder_path_to_include is not None:
            validate_folder_path_to_include(folder_path_to_include)
            fabric_workspace_obj.publish_folder_path_to_include = folder_path_to_include

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
        validate_items_to_include(items_to_include, operation=constants.OperationType.PUBLISH)
        fabric_workspace_obj.items_to_include = items_to_include

    if shortcut_exclude_regex:
        validate_shortcut_exclude_regex(shortcut_exclude_regex)
        fabric_workspace_obj.shortcut_exclude_regex = shortcut_exclude_regex

    # Publish items in the defined order synchronously
    total_item_types = len(constants.SERIAL_ITEM_PUBLISH_ORDER)
    publishers_with_async_check: list[items.ItemPublisher] = []
    for order_num, item_type in items.ItemPublisher.get_item_types_to_publish(fabric_workspace_obj):
        log_header(logger, f"Publishing Item {order_num}/{total_item_types}: {item_type.value}")
        publisher = items.ItemPublisher.create(item_type, fabric_workspace_obj)
        publisher.publish_all()
        if publisher.has_async_publish_check:
            publishers_with_async_check.append(publisher)

    # Check asynchronous publish status for relevant item types
    for publisher in publishers_with_async_check:
        log_header(logger, f"Checking {publisher.item_type} Publish State")
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
) -> DeploymentResult:
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

    Returns:
        DeploymentResult: A result object containing the deployment status, message, and
            responses (opt-in). The status will be DeploymentStatus.COMPLETED on success.
            The responses field contains API response data when the
            ``enable_response_collection`` feature flag is enabled and responses were collected,
            otherwise None.

    Raises:
        InputError: If configuration is invalid, environment not found, or input validation fails.
        ConfigValidationError: If configuration file is missing or fails structural validation.

    Note:
        On failure, the raised exception will have a ``deployment_result`` attribute
        containing a ``DeploymentResult`` with ``status`` set to
        ``DeploymentStatus.FAILED``, ``message`` set to the error description, and
        ``responses`` containing any partial API responses collected before the failure
        (requires the ``enable_response_collection`` feature flag, otherwise None).

    Examples:
        Basic usage
        >>> from fabric_cicd import deploy_with_config
        >>> result = deploy_with_config(
        ...     config_file_path="workspace/config.yml",
        ...     environment="prod"
        ... )
        >>> print(result.status)    # DeploymentStatus.COMPLETED
        >>> print(result.message)   # "Deployment completed successfully"
        >>> print(result.responses) # API responses if collected and feature flag enabled

        With custom authentication
        >>> from fabric_cicd import deploy_with_config
        >>> from azure.identity import ClientSecretCredential
        >>> credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        >>> result = deploy_with_config(
        ...     config_file_path="workspace/config.yml",
        ...     environment="prod",
        ...     token_credential=credential
        ... )

        With override configuration
        >>> from fabric_cicd import deploy_with_config
        >>> from azure.identity import ClientSecretCredential
        >>> credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        >>> result = deploy_with_config(
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

        Handling deployment failures
        >>> from fabric_cicd import deploy_with_config
        >>> try:
        ...     result = deploy_with_config(
        ...         config_file_path="workspace/config.yml",
        ...         environment="prod"
        ...     )
        ...     print(result.status)    # DeploymentStatus.COMPLETED
        ...     print(result.message)   # "Deployment completed successfully"
        ...     print(result.responses) # API responses if collected (feature flag enabled via config file)
        ... except Exception as e:
        ...     print(e.deployment_result.status)    # DeploymentStatus.FAILED
        ...     print(e.deployment_result.message)   # Original error message
        ...     print(e.deployment_result.responses) # Partial API responses or None
    """
    log_header(logger, "Config-Based Deployment")
    logger.info(f"Loading configuration from {config_file_path} for environment '{environment}'")

    # Initialize workspace as None so it exists in except block scope
    workspace = None
    responses_enabled = False

    try:
        # Validate environment
        environment = validate_environment(environment)

        # Load and validate configuration file
        config = load_config_file(config_file_path, environment, config_override)

        # Extract environment-specific settings
        workspace_settings = extract_workspace_settings(config, environment)
        publish_settings = extract_publish_settings(config, environment)
        unpublish_settings = extract_unpublish_settings(config, environment)

        # Apply feature flags and constants if specified
        with config_overrides_scope(config, environment):
            # Determine if response collection flag has been enabled in the config file
            responses_enabled = FeatureFlag.ENABLE_RESPONSE_COLLECTION.value in constants.FEATURE_FLAG

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
                    folder_path_to_include=publish_settings.get("folder_path_to_include"),
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

    except Exception as e:
        e.deployment_result = DeploymentResult(
            status=DeploymentStatus.FAILED,
            message=str(e),
            responses=_collect_responses(workspace, responses_enabled),
        )
        raise

    logger.info("Config-based deployment completed successfully")
    return DeploymentResult(
        status=DeploymentStatus.COMPLETED,
        message="Deployment completed successfully",
        responses=_collect_responses(workspace, responses_enabled),
    )


def _collect_responses(workspace: Optional[FabricWorkspace], responses_enabled: bool) -> Optional[dict]:
    """Return collected API responses if available, otherwise None."""
    if responses_enabled and workspace is not None and workspace.responses:
        return workspace.responses
    return None


def _find_platform_item(file_path: Path, repo_root: Path) -> Optional[tuple[str, str]]:
    """
    Walk up from file_path towards repo_root looking for a .platform file.

    The .platform file marks the boundary of a Fabric item directory.
    Its JSON content contains ``metadata.type`` (item type) and
    ``metadata.displayName`` (item name).

    Returns:
        A ``(item_name, item_type)`` tuple, or ``None`` if not found.
    """
    current = file_path.parent
    while True:
        platform_file = current / ".platform"
        if platform_file.exists():
            try:
                data = json.loads(platform_file.read_text(encoding="utf-8"))
                metadata = data.get("metadata", {})
                item_type = metadata.get("type")
                item_name = metadata.get("displayName") or current.name
                if item_type:
                    return item_name, item_type
            except Exception as exc:
                logger.debug(f"Could not parse .platform file at '{platform_file}': {exc}")
        # Stop if we have reached the repository root or the filesystem root
        if current == repo_root or current == current.parent:
            break
        current = current.parent
    return None


def get_changed_items(
    repository_directory: Path,
    git_compare_ref: str = "HEAD~1",
) -> list[str]:
    """
    Return the list of Fabric items that were added, modified, or renamed relative to ``git_compare_ref``.

    The returned list is in ``"item_name.item_type"`` format and can be passed directly
    to the ``items_to_include`` parameter of :func:`publish_all_items` to deploy only
    what has changed since the last commit.

    Args:
        repository_directory: Path to the local git repository directory
            (e.g. ``FabricWorkspace.repository_directory``).
        git_compare_ref: Git ref to compare against. Defaults to ``"HEAD~1"``.

    Returns:
        List of strings in ``"item_name.item_type"`` format. Returns an empty list when
        no changes are detected, the git root cannot be found, or git is unavailable.

    Examples:
        Deploy only changed items
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, get_changed_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Notebook", "DataPipeline"]
        ... )
        >>> changed = get_changed_items(workspace.repository_directory)
        >>> if changed:
        ...     publish_all_items(workspace, items_to_include=changed)

        With a custom git ref
        >>> changed = get_changed_items(workspace.repository_directory, git_compare_ref="main")
        >>> if changed:
        ...     publish_all_items(workspace, items_to_include=changed)
    """
    changed, _ = _resolve_changed_items(Path(repository_directory), git_compare_ref)
    return changed


def _resolve_changed_items(
    repository_directory: Path,
    git_compare_ref: str,
) -> tuple[list[str], list[str]]:
    """
    Use ``git diff --name-status`` to detect Fabric items that changed or were
    deleted relative to *git_compare_ref*.

    Args:
        repository_directory: Absolute path to the local repository directory
            (as stored on ``FabricWorkspace.repository_directory``).
        git_compare_ref: Git ref to diff against (e.g. ``"HEAD~1"``).

    Returns:
        A two-element tuple ``(changed_items, deleted_items)`` where each
        element is a list of strings in ``"item_name.item_type"`` format.
        Both lists are empty when the git root cannot be found or git fails.
    """
    from fabric_cicd._common._config_validator import _find_git_root

    git_root = _find_git_root(repository_directory)
    if git_root is None:
        logger.warning("get_changed_items: could not locate a git repository root — returning empty list.")
        return [], []

    try:
        result = subprocess.run(
            ["git", "diff", "--name-status", git_compare_ref],
            cwd=str(git_root),
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        logger.warning(f"get_changed_items: 'git diff' failed ({exc.stderr.strip()}) — returning empty list.")
        return [], []

    changed_items: set[str] = set()
    deleted_items: set[str] = set()

    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        status = parts[0].strip()

        # Renames produce three tab-separated fields: R<score>\told\tnew
        if status.startswith("R") and len(parts) >= 3:
            file_path_str = parts[2]
        elif len(parts) >= 2:
            file_path_str = parts[1]
        else:
            continue

        abs_path = git_root / file_path_str

        # Only consider files inside the configured repository directory
        try:
            abs_path.relative_to(repository_directory)
        except ValueError:
            continue

        if status == "D":
            # For deleted items: if the .platform file itself was deleted, we can
            # recover item metadata from the old commit via `git show`.
            if abs_path.name == ".platform":
                try:
                    show_result = subprocess.run(
                        ["git", "show", f"{git_compare_ref}:{file_path_str}"],
                        cwd=str(git_root),
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    data = json.loads(show_result.stdout)
                    metadata = data.get("metadata", {})
                    item_type = metadata.get("type")
                    item_name = metadata.get("displayName") or abs_path.parent.name
                    if item_type and item_name:
                        deleted_items.add(f"{item_name}.{item_type}")
                except Exception as exc:
                    logger.debug(f"get_changed_items: could not read deleted .platform '{file_path_str}': {exc}")
        else:
            # Modified / Added / Copied / Renamed — walk up to find the .platform
            item_info = _find_platform_item(abs_path, repository_directory)
            if item_info:
                changed_items.add(f"{item_info[0]}.{item_info[1]}")

    return list(changed_items), list(deleted_items)
