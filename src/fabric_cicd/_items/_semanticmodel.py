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

    # Use Power BI API to get dataset information
    # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-datasets-in-group
    powerbi_url = f"{constants.DEFAULT_API_ROOT_URL}/v1.0/myorg/groups/{fabric_workspace_obj.workspace_id}/datasets"
    response = fabric_workspace_obj.endpoint.invoke(method="GET", url=powerbi_url)

    datasets = response.get("body", {}).get("value", [])

    datasets_with_gateway = [ds for ds in datasets if ds.get("isOnPremGatewayRequired")]

    gateway_dict = fabric_workspace_obj.environment_parameter.get("gateway_binding", [])
    # Build gateway mapping once
    gateway_mapping = {}
    for gateway in gateway_dict:
        gateway_id = gateway.get("gateway_id")
        dataset_name = gateway.get("dataset_name", [])

        if isinstance(dataset_name, str):
            dataset_name = [dataset_name]

        for _d in dataset_name:
            gateway_mapping[_d] = gateway_id

    for dataset in datasets_with_gateway:
        semantic_model_name = dataset.get("name")
        gateway_id = gateway_mapping.get(semantic_model_name)

        if gateway_id:
            logger.info(f"Binding semantic model '{semantic_model_name}' to gateway ID '{gateway_id}'")
            status_code = bind_semanticmodel_to_gateway(
                fabric_workspace_obj=fabric_workspace_obj, datasets_id=dataset.get("id"), gateway_id=gateway_id
            )
            if status_code == 200:
                logger.info(
                    f"Successfully bound semantic model '{semantic_model_name}' to gateway. Status code: {status_code}"
                )
            else:
                logger.info(
                    f"Failed to bind semantic model '{semantic_model_name}' to gateway. Status code: {status_code}"
                )
        else:
            logger.warning(f"No gateway binding found for semantic model: {semantic_model_name}")


def bind_semanticmodel_to_gateway(fabric_workspace_obj: FabricWorkspace, datasets_id: str, gateway_id: str) -> str:
    """
    Binds a semantic model to a specified gateway.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
        datasets_id: The ID of the dataset to bind.
        gateway_id: The ID of the gateway to bind to.
    """
    powerbi_url = f"{constants.DEFAULT_API_ROOT_URL}/v1.0/myorg/groups/{fabric_workspace_obj.workspace_id}/datasets/{datasets_id}/Default.BindToGateway"
    body = {"gatewayId": gateway_id}
    return fabric_workspace_obj.endpoint.invoke(method="POST", url=powerbi_url, body=body)["status_code"]
