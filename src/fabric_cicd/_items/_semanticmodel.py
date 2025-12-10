# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Semantic Model item."""

import logging
from typing import Optional

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
        _publish_semanticmodel_with_retry(
            fabric_workspace_obj=fabric_workspace_obj,
            item_name=item_name,
            item_type=item_type,
            exclude_path=exclude_path,
        )

    model_with_binding_dict = fabric_workspace_obj.environment_parameter.get("semantic_model_binding", [])

    if not model_with_binding_dict:
        # Check if semantic model refresh is configured
        _refresh_semanticmodels_if_configured(fabric_workspace_obj)
        return

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

    # Refresh semantic models after binding if configured
    _refresh_semanticmodels_if_configured(fabric_workspace_obj)


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


def _is_destructive_change_error(error_message: str, error_code: Optional[str] = None) -> bool:
    """
    Check if an error indicates a destructive change that requires data purge.

    Args:
        error_message: The error message from the API response
        error_code: Optional error code from the API response

    Returns:
        True if the error indicates destructive changes, False otherwise
    """
    # Check for known destructive change error codes
    destructive_error_codes = [
        "Alm_InvalidRequest_PurgeRequired",
        "PurgeRequired",
    ]

    if error_code and error_code in destructive_error_codes:
        return True

    # Check for destructive change keywords in error message
    destructive_keywords = [
        "purge required",
        "data deletion",
        "destructive change",
        "will cause loss of data",
        "requires data to be dropped",
    ]

    error_message_lower = error_message.lower() if error_message else ""
    return any(keyword in error_message_lower for keyword in destructive_keywords)


def _publish_semanticmodel_with_retry(
    fabric_workspace_obj: FabricWorkspace,
    item_name: str,
    item_type: str,
    exclude_path: str,
) -> None:
    """
    Publishes a semantic model with retry logic for destructive changes.

    This function attempts to publish a semantic model. If it fails due to destructive
    changes requiring data purge, it logs a detailed warning with guidance on how to
    resolve the issue.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        item_name: Name of the semantic model to publish
        item_type: Type of the item (SemanticModel)
        exclude_path: Regex string of paths to exclude
    """
    try:
        # Try to publish the semantic model normally
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, exclude_path=exclude_path)
    except Exception as e:
        error_message = str(e)

        # Check if this is a destructive change error
        if _is_destructive_change_error(error_message):
            logger.warning(
                f"Semantic model '{item_name}' deployment failed due to destructive changes that require data purge."
            )
            logger.warning(
                "Destructive changes include operations like: removing columns, changing data types, "
                "altering partition definitions, or removing hierarchies."
            )
            logger.warning(
                "To resolve this issue, you have the following options:\n"
                "  1. Use external tools to clear values before deployment:\n"
                "     - Connect via XMLA endpoint (e.g., using Tabular Editor or SSMS)\n"
                "     - Execute TMSL command: {'refresh': {'type': 'clearValues', 'objects': [...]}}\n"
                "  2. Manually delete and recreate the semantic model in the target workspace\n"
                "  3. Review the schema changes and revert incompatible modifications"
            )
            logger.error(f"Full error details: {error_message}")

        # Re-raise the exception so deployment fails visibly
        raise


def _refresh_semanticmodels_if_configured(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Refresh semantic models if configured in parameter file.

    Checks for 'semantic_model_refresh' parameter and refreshes models accordingly.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
    """
    refresh_config = fabric_workspace_obj.environment_parameter.get("semantic_model_refresh")

    if not refresh_config:
        return

    item_type = "SemanticModel"

    # Get list of semantic models to refresh
    models_to_refresh = []

    # Check if refresh_config is a list (multiple model configurations)
    if isinstance(refresh_config, list):
        for config in refresh_config:
            model_names = config.get("semantic_model_name", [])
            if isinstance(model_names, str):
                model_names = [model_names]

            refresh_payload = config.get("refresh_payload")

            for model_name in model_names:
                models_to_refresh.append({"name": model_name, "payload": refresh_payload})
    # Single model configuration as dict
    elif isinstance(refresh_config, dict):
        model_names = refresh_config.get("semantic_model_name", [])
        if isinstance(model_names, str):
            model_names = [model_names]

        refresh_payload = refresh_config.get("refresh_payload")

        for model_name in model_names:
            models_to_refresh.append({"name": model_name, "payload": refresh_payload})

    # Refresh each model
    for model_config in models_to_refresh:
        model_name = model_config["name"]
        custom_payload = model_config.get("payload")

        # Check if this semantic model exists in the repository
        if model_name not in fabric_workspace_obj.repository_items.get(item_type, {}):
            logger.warning(f"Semantic model '{model_name}' not found in repository, skipping refresh")
            continue

        # Get the semantic model object
        item_obj = fabric_workspace_obj.repository_items[item_type][model_name]
        model_id = item_obj.guid

        if not model_id:
            logger.warning(f"Semantic model '{model_name}' has no GUID, skipping refresh")
            continue

        _refresh_semanticmodel(
            fabric_workspace_obj=fabric_workspace_obj,
            model_name=model_name,
            model_id=model_id,
            custom_payload=custom_payload,
        )


def _refresh_semanticmodel(
    fabric_workspace_obj: FabricWorkspace,
    model_name: str,
    model_id: str,
    custom_payload: Optional[dict] = None,
) -> None:
    """
    Refresh a semantic model using Power BI REST API.

    Args:
        fabric_workspace_obj: The FabricWorkspace object
        model_name: Name of the semantic model
        model_id: GUID of the semantic model
        custom_payload: Optional custom refresh payload. If None, uses default automatic refresh.
    """
    logger.info(f"Refreshing semantic model '{model_name}' (ID: {model_id})")

    # Build the refresh payload
    if custom_payload:
        refresh_body = custom_payload
        logger.debug(f"Using custom refresh payload for '{model_name}': {refresh_body}")
    else:
        # Default to automatic/default refresh (no payload needed for basic refresh)
        refresh_body = {"type": "full"}
        logger.debug(f"Using default full refresh for '{model_name}'")

    try:
        # Use Power BI API for dataset refresh
        # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset
        refresh_url = (
            f"{constants.DEFAULT_API_ROOT_URL}/v1.0/myorg/groups/"
            f"{fabric_workspace_obj.workspace_id}/datasets/{model_id}/refreshes"
        )

        refresh_response = fabric_workspace_obj.endpoint.invoke(
            method="POST",
            url=refresh_url,
            body=refresh_body,
        )

        status_code = refresh_response.get("status_code")

        if status_code == 202:
            logger.info(f"{constants.INDENT}Refresh initiated successfully for '{model_name}'")
            # Note: 202 means the refresh has been accepted and is running asynchronously
        elif status_code == 200:
            logger.info(f"{constants.INDENT}Refresh completed for '{model_name}'")
        else:
            logger.warning(f"{constants.INDENT}Unexpected status code for refresh: {status_code}")

    except Exception as e:
        logger.error(f"Failed to refresh semantic model '{model_name}': {e!s}")
        # Don't re-raise - we want deployment to continue even if refresh fails
