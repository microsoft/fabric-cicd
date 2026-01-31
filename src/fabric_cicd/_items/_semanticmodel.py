# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Semantic Model item."""

import logging

from fabric_cicd import FabricWorkspace, constants

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

    # Handle semantic_model_binding for connection binding
    model_with_binding_dict = fabric_workspace_obj.environment_parameter.get("semantic_model_binding", [])

    if model_with_binding_dict:
        # Build connection mapping from semantic_model_binding parameter
        binding_mapping = {}

        for model in model_with_binding_dict:
            model_name = model.get("semantic_model_name", [])
            connection_id = model.get("connection_id")

            if isinstance(model_name, str):
                model_name = [model_name]

            for name in model_name:
                binding_mapping[name] = connection_id

        connections = get_connections(fabric_workspace_obj)

        if binding_mapping:
            bind_semanticmodel_to_connection(
                fabric_workspace_obj=fabric_workspace_obj, connections=connections, connection_details=binding_mapping
            )

    # Handle semantic_model_parameters for parameter updates
    model_with_parameters_dict = fabric_workspace_obj.environment_parameter.get("semantic_model_parameters", [])

    if model_with_parameters_dict:
        update_semantic_model_parameters(
            fabric_workspace_obj=fabric_workspace_obj, parameter_details=model_with_parameters_dict
        )


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
    fabric_workspace_obj: FabricWorkspace, connections: dict, connection_details: dict
) -> None:
    """
    Binds semantic models to their specified connections.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        connections: Dictionary of connection objects with connection ID as key.
        connection_details: Dictionary mapping dataset names to connection IDs from parameter.yml.
    """
    item_type = "SemanticModel"

    # Loop through each semantic model in the semantic_model_binding section
    for dataset_name, connection_id in connection_details.items():
        # Check if the connection ID exists in the connections dict
        if connection_id not in connections:
            logger.warning(f"Connection ID '{connection_id}' not found for semantic model '{dataset_name}'")
            continue

        # Check if this semantic model exists in the repository
        if dataset_name not in fabric_workspace_obj.repository_items.get(item_type, {}):
            logger.warning(f"Semantic model '{dataset_name}' not found in repository")
            continue

        # Get the semantic model object
        item_obj = fabric_workspace_obj.repository_items[item_type][dataset_name]
        model_id = item_obj.guid

        logger.info(f"Binding semantic model '{dataset_name}' (ID: {model_id}) to connection '{connection_id}'")

        try:
            # Get the connection details for this semantic model from Fabric API
            item_connections_url = f"{constants.FABRIC_API_ROOT_URL}/v1/workspaces/{fabric_workspace_obj.workspace_id}/items/{model_id}/connections"
            connections_response = fabric_workspace_obj.endpoint.invoke(method="GET", url=item_connections_url)
            connections_data = connections_response.get("body", {}).get("value", [])

            if not connections_data:
                logger.warning(f"No connections found for semantic model '{dataset_name}'")
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
                logger.warning(f"Failed to bind semantic model '{dataset_name}'. Status code: {status_code}")

        except Exception as e:
            logger.error(f"Failed to bind semantic model '{dataset_name}' to connection: {e!s}")
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


def update_semantic_model_parameters(fabric_workspace_obj: FabricWorkspace, parameter_details: list) -> None:
    """
    Updates parameters for semantic models using the Power BI REST API.

    This function updates dataset parameters (such as data source URLs, connection strings,
    database names, etc.) for deployed semantic models based on environment-specific
    configurations defined in the parameter.yml file.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        parameter_details: List of parameter configuration dictionaries from parameter.yml,
                          each containing semantic_model_name and parameters to update.

    Example parameter.yml structure:
        semantic_model_parameters:
          - semantic_model_name: "Sales Model"
            parameters:
              - name: "ServerName"
                new_value: "prod-sql-server.database.windows.net"
              - name: "DatabaseName"
                new_value: "SalesDB"
          - semantic_model_name: ["Finance Model", "HR Model"]
            parameters:
              - name: "ApiEndpoint"
                new_value: "https://api.production.com"
    """
    item_type = "SemanticModel"

    # Build parameter mapping from semantic_model_parameters configuration
    parameter_mapping = {}

    for entry in parameter_details:
        model_names = entry.get("semantic_model_name", [])
        parameters = entry.get("parameters", [])

        # Convert single model name to list for uniform processing
        if isinstance(model_names, str):
            model_names = [model_names]

        # Map each model name to its parameters
        for model_name in model_names:
            parameter_mapping[model_name] = parameters

    # Process each semantic model
    for model_name, parameters in parameter_mapping.items():
        # Check if this semantic model exists in the repository
        if model_name not in fabric_workspace_obj.repository_items.get(item_type, {}):
            logger.warning(f"Semantic model '{model_name}' not found in repository, skipping parameter update")
            continue

        # Get the semantic model object
        item_obj = fabric_workspace_obj.repository_items[item_type][model_name]
        model_id = item_obj.guid

        logger.info(f"Updating parameters for semantic model '{model_name}' (ID: {model_id})")

        try:
            # Build the parameter update request
            update_details = []
            for param in parameters:
                param_name = param.get("name")
                param_value = param.get("new_value")

                if not param_name:
                    logger.warning(f"Parameter missing 'name' for semantic model '{model_name}', skipping")
                    continue

                if param_value is None:
                    logger.warning(
                        f"Parameter '{param_name}' missing 'new_value' for semantic model '{model_name}', skipping"
                    )
                    continue

                update_details.append({"name": param_name, "newValue": str(param_value)})
                logger.debug(f"  - Setting parameter '{param_name}' = '{param_value}'")

            if not update_details:
                logger.warning(f"No valid parameters to update for semantic model '{model_name}'")
                continue

            # Construct the Power BI API endpoint
            powerbi_url = f"{constants.DEFAULT_API_ROOT_URL}/v1.0/myorg/groups/{fabric_workspace_obj.workspace_id}/datasets/{model_id}/Default.UpdateParameters"

            # Build request body
            request_body = {"updateDetails": update_details}

            # Make the API call to update parameters
            response = fabric_workspace_obj.endpoint.invoke(method="POST", url=powerbi_url, body=request_body)

            status_code = response.get("status_code")

            if status_code == 200:
                logger.info(
                    f"Successfully updated {len(update_details)} parameter(s) for semantic model '{model_name}'"
                )
            else:
                error_message = response.get("body", {}).get("error", {}).get("message", "Unknown error")
                logger.warning(
                    f"Failed to update parameters for semantic model '{model_name}'. "
                    f"Status code: {status_code}, Error: {error_message}"
                )

        except Exception as e:
            logger.error(f"Failed to update parameters for semantic model '{model_name}': {e!s}")
            continue
