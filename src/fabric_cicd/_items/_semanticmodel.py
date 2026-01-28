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

    # Bind semantic models to connections after deploying (post-deployment step)
    semantic_model_binding = fabric_workspace_obj.environment_parameter.get("semantic_model_binding", {})
    if semantic_model_binding:
        environment = fabric_workspace_obj.environment

        # Check if legacy format (list) or new format (dict)
        if isinstance(semantic_model_binding, list):
            binding_mapping = build_binding_mapping_legacy(fabric_workspace_obj, semantic_model_binding)
        else:
            binding_mapping = build_binding_mapping(fabric_workspace_obj, semantic_model_binding, environment)

        if binding_mapping:
            connections = get_connections(fabric_workspace_obj)
            bind_semanticmodel_to_connection(
                fabric_workspace_obj=fabric_workspace_obj, connections=connections, connection_details=binding_mapping
            )


def build_binding_mapping_legacy(fabric_workspace_obj: FabricWorkspace, semantic_model_binding: list) -> dict:
    """
    Build the connection mapping from legacy list-based semantic_model_binding parameter.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        semantic_model_binding: The semantic_model_binding parameter as a list

    Returns:
        Dictionary mapping semantic model names to connection IDs
    """
    logger.warning(
        "The legacy 'semantic_model_binding' parameter format will be deprecated in a future release. "
        "Please migrate to the new dictionary format with 'default' and 'models' keys."
    )
    item_type = "SemanticModel"
    binding_mapping = {}
    repository_models = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())

    for entry in semantic_model_binding:
        connection_id = entry.get("connection_id")
        model_names = entry.get("semantic_model_name", [])

        if not connection_id:
            logger.debug("No connection_id found in semantic_model_binding entry, skipping")
            continue

        if isinstance(model_names, str):
            model_names = [model_names]

        for name in model_names:
            if name not in repository_models:
                logger.warning(f"Semantic model '{name}' specified in parameter.yml not found in repository")
                continue
            binding_mapping[name] = connection_id

    return binding_mapping


def build_binding_mapping(
    fabric_workspace_obj: FabricWorkspace, semantic_model_binding: dict, environment: str
) -> dict:
    """
    Build the connection mapping from semantic_model_binding parameter.
    Supports:
    - default.connections: Applied to all models in the repository with connections that are not explicitly listed
    - models: List of explicit model-to-connection mappings

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        semantic_model_binding: The semantic_model_binding parameter dictionary
        environment: The target environment name (_ALL_ key can be used)

    Returns:
        Dictionary mapping semantic model names to connection IDs
    """
    item_type = "SemanticModel"
    binding_mapping = {}
    repository_models = set(fabric_workspace_obj.repository_items.get(item_type, {}).keys())

    # Get default connection for this environment
    default_connections = semantic_model_binding.get("default", {}).get("connections", {})
    default_connections = process_environment_key(fabric_workspace_obj, default_connections)
    default_connection_id = default_connections.get(environment)
    if not default_connection_id:
        logger.debug(f"Environment '{environment}' not found in default connections")

    # Process explicit model bindings
    explicit_models = set()
    models_config = semantic_model_binding.get("models", [])

    for model in models_config:
        model_names = model.get("semantic_model_name", [])
        connections = model.get("connections", {})

        if isinstance(model_names, str):
            model_names = [model_names]

        connections = process_environment_key(fabric_workspace_obj, connections)
        connection_id = connections.get(environment)
        if not connection_id:
            logger.debug(f"Environment '{environment}' not found in connections for semantic model(s): {model_names}")
            continue

        # Only track as explicit if environment connection is defined
        explicit_models.update(model_names)

        for name in model_names:
            if name not in repository_models:
                logger.warning(f"Semantic model '{name}' specified in parameter.yml not found in repository")
                continue
            binding_mapping[name] = connection_id

    # Apply default connection to non-explicit models using set difference
    if default_connection_id:
        default_models = repository_models - explicit_models
        for model_name in default_models:
            binding_mapping[model_name] = default_connection_id
            logger.debug(f"Applying default connection to semantic model '{model_name}'")

    return binding_mapping


def get_connections(fabric_workspace_obj: FabricWorkspace) -> dict:
    """
    Get all connections from the workspace.

    Args:
        fabric_workspace_obj: The FabricWorkspace object

    Returns:
        Dictionary with connection ID as key and connection details as value
    """
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-connections
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
    fabric_workspace_obj: FabricWorkspace, connections: dict, connection_details: dict
) -> None:
    """
    Binds semantic models to their specified connections.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        connections: Dictionary of connection objects with connection ID as key.
        connection_details: Dictionary mapping semantic model names to connection IDs from parameter.yml.
    """
    item_type = "SemanticModel"

    for model_name, connection_id in connection_details.items():
        # Check if the connection ID exists in the connections dict
        if connection_id not in connections:
            logger.warning(f"Connection ID '{connection_id}' not found for semantic model '{model_name}'")
            continue

        # Get the semantic model object (validated during binding mapping creation)
        item_obj = fabric_workspace_obj.repository_items[item_type][model_name]
        model_id = item_obj.guid

        logger.info(f"Binding semantic model '{model_name}' (ID: {model_id}) to connection '{connection_id}'")

        try:
            # Get the connection details for this semantic model from Fabric API
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-item-connections
            item_connections_url = f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}/items/{model_id}/connections"
            connections_response = fabric_workspace_obj.endpoint.invoke(method="GET", url=item_connections_url)
            connections_data = connections_response.get("body", {}).get("value", [])

            if not connections_data:
                logger.debug(f"No existing connections found for semantic model '{model_name}', skipping binding")
                continue

            # Use the first connection as the template
            connection_binding = connections_data[0]

            # Update the connection binding with the target connection ID from parameter.yml
            connection_binding["id"] = connection_id
            connection_binding["connectivityType"] = connections[connection_id]["connectivityType"]
            connection_binding["connectionDetails"] = connections[connection_id]["connectionDetails"]

            # Build the request body
            request_body = build_request_body({"connectionBinding": connection_binding})

            # Make the bind connection API call
            # https://learn.microsoft.com/en-us/rest/api/fabric/semanticmodel/items/bind-semantic-model-connection
            binding_url = f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}/semanticModels/{model_id}/bindConnection"
            bind_response = fabric_workspace_obj.endpoint.invoke(
                method="POST",
                url=binding_url,
                body=request_body,
            )

            status_code = bind_response.get("status_code")

            if status_code == 200:
                logger.info(f"Successfully bound semantic model '{model_name}' to connection '{connection_id}'")
            else:
                logger.warning(f"Failed to bind semantic model '{model_name}'. Status code: {status_code}")

        except Exception as e:
            logger.error(f"Failed to bind semantic model '{model_name}' to connection: {e!s}")
            continue


def build_request_body(body: dict) -> dict:
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
