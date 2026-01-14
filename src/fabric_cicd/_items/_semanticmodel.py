# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Semantic Model item."""

import logging

from fabric_cicd import FabricWorkspace, constants
from fabric_cicd._parameter._utils import process_environment_key

logger = logging.getLogger(__name__)


def publish_semanticmodels(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all semantic model items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "SemanticModel"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        exclude_path = r".*\.pbi[/\\].*"
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, exclude_path=exclude_path)

    # Post-deployment step: bind Semantic Models to connections (gateway or cloud connections)
    binding_config = fabric_workspace_obj.environment_parameter.get("semantic_model_binding", {})
    if not binding_config:
        return

    # Build binding mapping with support for new structure
    binding_mapping = build_binding_mapping(fabric_workspace_obj, item_type, binding_config)

    # Execute binding
    if binding_mapping:
        bind_semanticmodel_to_connection(
            fabric_workspace_obj=fabric_workspace_obj,
            item_type=item_type,
            binding_mapping=binding_mapping,
            # Get connections from workspace
            connections=get_connections(fabric_workspace_obj),
        )


"""Build the binding mapping from semantic_model_binding parameter"""


def build_binding_mapping(fabric_workspace_obj: FabricWorkspace, item_type: str, binding_config: dict) -> dict:
    """
    Build the binding mapping from semantic_model_binding parameter.
    Supports both new structure (default/models) and legacy structure for backward compatibility.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        item_type: The item type (should be "SemanticModel")
        binding_config: The semantic_model_binding configuration

    Returns:
        Dictionary mapping model names to tuple of (list of connection IDs, is_from_default)
    """
    environment = fabric_workspace_obj.environment
    binding_mapping = {}
    repository_models = fabric_workspace_obj.repository_items.get(item_type, {})

    # Check if using new structure (has 'default' or 'models' keys)
    has_new_structure = any(key in binding_config for key in ["default", "models"])

    if has_new_structure:
        # First process all semantic models defined under `models`
        configured_models = _process_new_structure(binding_config, environment, repository_models, binding_mapping)

        # Then apply default connection to all models NOT explicitly configured
        _apply_defaults(binding_config, environment, repository_models, configured_models, binding_mapping)
    else:
        # Legacy structure - backward compatibility (uses list format)
        _process_legacy_structure(binding_config, environment, repository_models, binding_mapping)

    return binding_mapping


def _process_new_structure(
    binding_config: dict,
    environment: str,
    repository_models: dict,
    binding_mapping: dict,
) -> set:
    """Processes and adds explicitly configured semantic models from the new structure."""
    configured_models = set()
    models_list = binding_config.get("models", [])

    for model in models_list:
        model_names = model.get("semantic_model_name", [])
        connections_config = model.get("connections")

        if not connections_config:
            logger.debug("Skipping semantic model config with no connections defined")
            continue

        # Process each model's connection bindings
        models_configured = _process_model_configuration(
            model_names=model_names,
            connections_config=connections_config,
            environment=environment,
            repository_models=repository_models,
            binding_mapping=binding_mapping,
            is_default=False,
        )
        configured_models.update(models_configured)

    # Return a set of model names that were successfully configured
    return configured_models


def _apply_defaults(
    binding_config: dict,
    environment: str,
    repository_models: dict,
    configured_models: set,
    binding_mapping: dict,
) -> None:
    """
    Applies the default connection configuration to all semantic models in the repository
    that are not defined under 'models'. Skips models that have already been configured or
    don't require binding.
    """
    default_connections = binding_config.get("default", {}).get("connections")
    if not default_connections:
        return

    unconfigured_models = [name for name in repository_models if name not in configured_models]

    if unconfigured_models:
        _process_model_configuration(
            model_names=unconfigured_models,
            connections_config=default_connections,
            environment=environment,
            repository_models=repository_models,
            binding_mapping=binding_mapping,
            is_default=True,
        )


def _process_legacy_structure(
    binding_config: dict,
    environment: str,
    repository_models: dict,
    binding_mapping: dict,
) -> None:
    """Process legacy semantic model binding parameter structure with connection_id mappings."""
    for model in binding_config:
        model_names = model.get("semantic_model_name", [])
        connection_id = model.get("connection_id")

        if not connection_id:
            continue

        _process_model_configuration(
            model_names=model_names,
            connections_config=connection_id,
            environment=environment,
            repository_models=repository_models,
            binding_mapping=binding_mapping,
            is_default=False,
        )


def _process_model_configuration(
    model_names: list | str,
    connections_config: dict | str,
    environment: str,
    repository_models: dict,
    binding_mapping: dict,
    is_default: bool = False,
) -> set:
    """
    Common processing logic that validates model names against the repository,
    resolves environment-specific connections, and populates the binding mapping.
    Returns empty set if no valid models or connections are found.

    Args:
        model_names: Single model name (str) or list of model names to configure
        connections_config: Connection configuration (environment dict or single connection ID string)
        environment: Target environment name for connection resolution (e.g., "DEV", "PROD")
        repository_models: Dict of semantic models found in the repository
        binding_mapping: Dict to populate with model bindings (modified in place)
        is_default: Whether this binding is from default configuration (for logging purposes)

    Returns:
        Set of model names that were successfully configured with connections
    """
    if isinstance(model_names, str):
        model_names = [model_names]

    valid_model_names = [name for name in model_names if name in repository_models]

    invalid_models = set(model_names) - set(valid_model_names)
    for invalid_model in invalid_models:
        logger.warning(f"Semantic model '{invalid_model}' not found in repository")

    if not valid_model_names:
        return set()

    connection_ids = _resolve_connections(connections_config, environment)
    if not connection_ids:
        logger.warning(
            f"No valid connection IDs resolved for semantic models: {valid_model_names}. "
            f"Check environment '{environment}' configuration."
        )
        return set()

    configured_models = set()
    for model_name in valid_model_names:
        binding_mapping[model_name] = (connection_ids, is_default)
        configured_models.add(model_name)

    return configured_models


def _resolve_connections(connections_config: dict | str, environment: str) -> list:
    """
    Resolve connection IDs for a given environment.

    Args:
        connections_config: Connection configuration (dict with env keys or string)
        environment: Target environment

    Returns:
        List of connection IDs
    """
    if isinstance(connections_config, str):
        return [connections_config]

    if isinstance(connections_config, dict):
        processed_config = process_environment_key(environment, connections_config.copy())

        if environment not in processed_config:
            logger.warning(
                f"Environment '{environment}' not found in connections config. "
                f"Available environments: {list(processed_config.keys())}"
            )
            return []

        conn_value = processed_config[environment]

        # Handle list or single value
        if isinstance(conn_value, list):
            return conn_value
        if isinstance(conn_value, str):
            return [conn_value]

        logger.warning(f"Invalid connection value type: {type(conn_value)}")
        return []

    logger.warning(f"Invalid connections_config type: {type(connections_config)}")
    return []


"""Get all connections from the workspace"""


def get_connections(fabric_workspace_obj: FabricWorkspace) -> dict:
    """Get all connections from the workspace."""
    connections_url = f"{constants.FABRIC_API_ROOT_URL}/v1/connections"

    try:
        response = fabric_workspace_obj.endpoint.invoke(method="GET", url=connections_url)
        connections_list = response.get("body", {}).get("value", [])

        connections_dict = {}
        for connection in connections_list:
            connection_id = connection.get("id")
            if connection_id:
                connections_dict[connection_id] = {
                    "id": connection_id,
                    "connectivityType": connection.get("connectivityType"),
                    "connectionDetails": connection.get("connectionDetails", {}),
                }
        # Return a dict with connection ID as key and connection details as value
        return connections_dict
    except Exception as e:
        logger.error(f"Failed to retrieve connections: {e}")
        return {}


"""Bind semantic models to their specified connections"""


def bind_semanticmodel_to_connection(
    fabric_workspace_obj: FabricWorkspace,
    item_type: str,
    binding_mapping: dict,
    connections: dict,
) -> None:
    """
    Binds semantic models to their specified connections.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        item_type: The item type (should be "SemanticModel")
        binding_mapping: Dictionary mapping dataset names to tuple of (list of connection IDs, is_default).
        connections: Dictionary of connection objects with connection ID as key.
    """
    binding_stats = {"successful": 0, "failed": 0, "skipped": 0}

    for dataset_name, (connection_ids, is_default) in binding_mapping.items():
        item_obj = fabric_workspace_obj.repository_items[item_type][dataset_name]
        model_id = item_obj.guid

        # Get existing connections for this model
        existing_connections = _get_model_connections(fabric_workspace_obj, model_id, dataset_name, is_default)
        if not existing_connections:
            binding_stats["skipped"] += 1
            continue

        # Bind each configured connection
        for connection_id in connection_ids:
            success = _bind_single_connection(
                fabric_workspace_obj=fabric_workspace_obj,
                dataset_name=dataset_name,
                model_id=model_id,
                connection_id=connection_id,
                connections=connections,
                existing_connections=existing_connections,
            )
            if success:
                binding_stats["successful"] += 1
            else:
                binding_stats["failed"] += 1

    # Log summary
    logger.info(
        f"Connection binding complete: {binding_stats['successful']} successful, "
        f"{binding_stats['failed']} failed, {binding_stats['skipped']} skipped"
    )


def _get_model_connections(
    fabric_workspace_obj: FabricWorkspace,
    model_id: str,
    dataset_name: str,
    is_default: bool,
) -> list:
    """
    Retrieve existing connections for a semantic model.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        model_id: The semantic model GUID
        dataset_name: Name of the semantic model (for logging)
        is_default: Whether this is a default binding

    Returns:
        List of existing connection objects, empty list if none found or error occurs
    """
    try:
        item_connections_url = (
            f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/"
            f"{fabric_workspace_obj.workspace_id}/items/{model_id}/connections"
        )
        response = fabric_workspace_obj.endpoint.invoke(method="GET", url=item_connections_url)
        existing_connections = response.get("body", {}).get("value", [])

        if not existing_connections:
            if is_default:
                logger.debug(f"Skipping semantic model '{dataset_name}' - no connections found (applied from default)")
            else:
                logger.warning(f"No connections found for semantic model '{dataset_name}'")

        return existing_connections

    except Exception as e:
        logger.error(f"Failed to retrieve connections for semantic model '{dataset_name}': {e!s}")
        return []


def _bind_single_connection(
    fabric_workspace_obj: FabricWorkspace,
    dataset_name: str,
    model_id: str,
    connection_id: str,
    connections: dict,
    existing_connections: list,
) -> bool:
    """
    Bind a single connection to a semantic model.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        dataset_name: Name of the semantic model
        model_id: The semantic model GUID
        connection_id: Target connection ID to bind
        connections: Dictionary of available connections
        existing_connections: List of existing model connections

    Returns:
        True if binding succeeded, False otherwise
    """
    # Validate connection exists
    if connection_id not in connections:
        logger.warning(f"Connection ID '{connection_id}' not found for semantic model '{dataset_name}'")
        return False

    # Find matching connection template
    connection_binding = _find_matching_connection(
        target_connection=connections[connection_id],
        existing_connections=existing_connections,
        dataset_name=dataset_name,
    )

    if not connection_binding:
        return False

    # Update binding with target connection details
    target_connection = connections[connection_id]
    connection_binding.update({
        "id": connection_id,
        "connectivityType": target_connection["connectivityType"],
        "connectionDetails": target_connection["connectionDetails"],
    })

    # Execute binding
    return _execute_binding(
        fabric_workspace_obj=fabric_workspace_obj,
        model_id=model_id,
        dataset_name=dataset_name,
        connection_id=connection_id,
        connection_binding=connection_binding,
    )


def _find_matching_connection(
    target_connection: dict,
    existing_connections: list,
    dataset_name: str,
) -> dict | None:
    """
    Find matching connection from existing connections by connectivity type and details.

    Args:
        target_connection: Target connection configuration
        existing_connections: List of existing model connections
        dataset_name: Name of semantic model (for logging)

    Returns:
        Matching connection dict or None if no match found
    """
    target_type = target_connection["connectivityType"]
    target_details = target_connection.get("connectionDetails", {})

    # First try: Match by type AND connection details (path/server)
    for conn in existing_connections:
        if conn.get("connectivityType") != target_type:
            continue

        conn_details = conn.get("connectionDetails", {})
        # Match on path (for file-based connections)
        if "path" in target_details and "path" in conn_details:
            is_match = target_details["path"] == conn_details["path"]

        # Match on server (for database connections)
        if "server" in target_details and "server" in conn_details:
            is_match = target_details["server"] == conn_details["server"]

        # No matching criteria found
        is_match = False

        if is_match:
            logger.debug(f"Found exact connection match for '{dataset_name}' (type: {target_type})")
            return conn.copy()

    # Second try: Match by type only (fallback)
    for conn in existing_connections:
        if conn.get("connectivityType") == target_type:
            logger.warning(
                f"No exact connection match for '{dataset_name}'. Using first connection with type '{target_type}'"
            )
            return conn.copy()

    # No match found
    logger.error(f"No matching connection found for '{dataset_name}' (type: {target_type})")
    return None


def _execute_binding(
    fabric_workspace_obj: FabricWorkspace,
    model_id: str,
    dataset_name: str,
    connection_id: str,
    connection_binding: dict,
) -> bool:
    """
    Execute the connection binding API call.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        model_id: The semantic model GUID
        dataset_name: Name of semantic model (for logging)
        connection_id: Target connection ID
        connection_binding: Connection binding configuration

    Returns:
        True if binding succeeded, False otherwise
    """
    try:
        request_body = _build_request_body({"connectionBinding": connection_binding})
        bind_url = (
            f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/"
            f"{fabric_workspace_obj.workspace_id}/semanticModels/{model_id}/bindConnection"
        )

        logger.info(
            f"Binding semantic model '{dataset_name}' (ID: {model_id}) "
            f"to connection '{connection_id}' (Type: {connection_binding['connectivityType']})"
        )

        response = fabric_workspace_obj.endpoint.invoke(
            method="POST",
            url=bind_url,
            body=request_body,
        )

        status_code = response.get("status_code")
        if status_code == 200:
            logger.info(f"Successfully bound semantic model '{dataset_name}' to connection '{connection_id}'")
            return True

        logger.warning(
            f"Failed to bind semantic model '{dataset_name}' to connection '{connection_id}'. "
            f"Status code: {status_code}"
        )
        return False

    except Exception as e:
        logger.error(f"Failed to bind semantic model '{dataset_name}' to connection '{connection_id}': {e!s}")
        return False


def _build_request_body(body: dict) -> dict:
    """
    Build request body with specific order of fields for connection binding.

    Args:
        body: Dictionary containing connectionBinding data

    Returns:
        Ordered dictionary with id, connectivityType, and connectionDetails
    """
    connection_binding = body.get("connectionBinding", {})
    connection_details = connection_binding.get("connectionDetails", {})

    return {
        "connectionBinding": {
            "id": connection_binding.get("id"),
            "connectivityType": connection_binding.get("connectivityType"),
            "connectionDetails": {
                "type": connection_details.get("type") if "type" in connection_details else None,
                "path": connection_details.get("path") if "path" in connection_details else None,
            },
        }
    }
