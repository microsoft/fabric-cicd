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

    if binding_mapping:
        bind_semanticmodel_to_connection(
            fabric_workspace_obj=fabric_workspace_obj,
            item_type=item_type,
            binding_mapping=binding_mapping,
            # Get connections from workspace
            connections=get_connections(fabric_workspace_obj),
        )


def build_binding_mapping(fabric_workspace_obj: FabricWorkspace, item_type: str, binding_config: dict) -> dict:
    """
    Build the binding mapping from semantic_model_binding parameter.
    Supports both new structure (default/items) and legacy structure for backward compatibility.

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

    # Check if using new structure (has 'default' or 'items' keys)
    has_new_structure = any(key in binding_config for key in ["default", "items"])

    if has_new_structure:
        # Track models with explicit item configurations
        configured_models = set()

        # Process specific semantic model items first
        items_list = binding_config.get("items", [])
        for item in items_list:
            model_names = item.get("semantic_model_name", [])
            if isinstance(model_names, str):
                model_names = [model_names]

            connections_config = item.get("connections")
            if not connections_config:
                continue

            connection_ids = _resolve_connections(connections_config, environment)
            if connection_ids:
                for model_name in model_names:
                    # Check if model exists in repository
                    if model_name not in repository_models:
                        logger.warning(f"Semantic model '{model_name}' configured in items but not found in repository")
                        continue

                    binding_mapping[model_name] = (connection_ids, False)
                    configured_models.add(model_name)

        # Apply default to all models NOT explicitly configured
        default_connections = binding_config.get("default", {}).get("connections")
        if default_connections:
            default_connection_ids = _resolve_connections(default_connections, environment)
            if default_connection_ids:
                for model_name in repository_models:
                    if model_name not in configured_models:
                        binding_mapping[model_name] = (default_connection_ids, True)
    else:
        # Legacy structure - backward compatibility (list format)
        for item in binding_config:
            model_names = item.get("semantic_model_name", [])
            if isinstance(model_names, str):
                model_names = [model_names]

            connection_id = item.get("connection_id")
            if not connection_id:
                continue

            # Resolve environment-specific connection_id
            connection_ids = _resolve_connections(connection_id, environment)

            if not connection_ids:
                logger.warning(
                    f"Skipping semantic model binding for '{model_names}' - "
                    f"could not resolve connection_id for environment '{environment}'"
                )
                continue

            for model_name in model_names:
                # Check if model exists in repository
                if model_name not in repository_models:
                    logger.warning(f"Semantic model '{model_name}' not found in repository")
                    continue

                binding_mapping[model_name] = (connection_ids, False)

    return binding_mapping


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

    return []


def get_connections(fabric_workspace_obj: FabricWorkspace) -> dict:
    """
    Get all connections from the workspace.

    Args:
        fabric_workspace_obj: The FabricWorkspace object

    Returns:
        Dictionary with connection ID as key and connection details as value
    """
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

        return connections_dict
    except Exception as e:
        logger.error(f"Failed to retrieve connections: {e}")
        return {}


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
    # Loop through each semantic model in the binding mapping
    for dataset_name, (connection_ids, is_default) in binding_mapping.items():
        # Get the semantic model object
        item_obj = fabric_workspace_obj.repository_items[item_type][dataset_name]
        model_id = item_obj.guid

        # Get the existing connections for this semantic model
        try:
            item_connections_url = f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}/items/{model_id}/connections"
            connections_response = fabric_workspace_obj.endpoint.invoke(method="GET", url=item_connections_url)
            existing_connections = connections_response.get("body", {}).get("value", [])

            if not existing_connections:
                # Use debug log for default bindings, warning for explicit
                if is_default:
                    logger.debug(
                        f"Skipping semantic model '{dataset_name}' - no connections found (applied from default)"
                    )
                else:
                    logger.warning(f"No connections found for semantic model '{dataset_name}'")
                continue

        except Exception as e:
            logger.error(f"Failed to retrieve connections for semantic model '{dataset_name}': {e!s}")
            continue

        # Match and bind each connection
        for connection_id in connection_ids:
            # Check if the connection ID exists in the connections dict
            if connection_id not in connections:
                logger.warning(f"Connection ID '{connection_id}' not found for semantic model '{dataset_name}'")
                continue

            # Find matching connection template from existing connections
            # Match by connectivityType to support multiple data sources
            target_connectivity_type = connections[connection_id]["connectivityType"]
            connection_binding = None

            for existing_conn in existing_connections:
                if existing_conn.get("connectivityType") == target_connectivity_type:
                    connection_binding = existing_conn
                    break

            # If no match found by connectivity type, use first connection as fallback
            if not connection_binding:
                logger.warning(
                    f"No matching connectivity type '{target_connectivity_type}' found in model '{dataset_name}'. "
                    f"Using first available connection as template."
                )
                connection_binding = existing_connections[0]

            logger.info(
                f"Binding semantic model '{dataset_name}' (ID: {model_id}) to connection '{connection_id}' "
                f"(Type: {target_connectivity_type})"
            )

            # Update the connection binding with the target connection
            connection_binding["id"] = connection_id
            connection_binding["connectivityType"] = connections[connection_id]["connectivityType"]
            connection_binding["connectionDetails"] = connections[connection_id]["connectionDetails"]

            # Build the request body
            request_body = _build_request_body({"connectionBinding": connection_binding})

            # Make the bind connection API call
            try:
                powerbi_url = f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}/semanticModels/{model_id}/bindConnection"
                bind_response = fabric_workspace_obj.endpoint.invoke(
                    method="POST",
                    url=powerbi_url,
                    body=request_body,
                )

                status_code = bind_response.get("status_code")

                if status_code == 200:
                    logger.info(f"Successfully bound semantic model '{dataset_name}' to connection '{connection_id}'")
                else:
                    logger.warning(
                        f"Failed to bind semantic model '{dataset_name}' to connection '{connection_id}'. "
                        f"Status code: {status_code}"
                    )

            except Exception as e:
                logger.error(f"Failed to bind semantic model '{dataset_name}' to connection '{connection_id}': {e!s}")
                continue


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
