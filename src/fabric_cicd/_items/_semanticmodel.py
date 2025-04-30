# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Semantic Model item."""

import logging

import fabric_cicd.constants as constants
from fabric_cicd import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_semanticmodels(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all semantic model items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "SemanticModel"
    # Get a list of all existing connections the account used has access to.
    gatewayconn = fabric_workspace_obj._get_existing_connections()
    # get a list of gateways to be used in the current environment
    gateways = fabric_workspace_obj._get_gateways()

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        exclude_path = r".*\.pbi[/\\].*"
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, exclude_path=exclude_path)
        item_guid = fabric_workspace_obj._convert_name_to_id(
            item_type=item_type, generic_name=item_name, lookup_type="Deployment"
        )
        if item_guid is not None:
            fabric_workspace_obj._takeover_semanticmodel(item_guid=item_guid)
            # get the connections of the sematic model
            datasetconn = fabric_workspace_obj._get_semanticmodel_connections(item_guid=item_guid)
            foundconn = find_datasource_connections(gateways, gatewayconn, datasetconn)
            # If the connections are found on one of the gateways
            if foundconn != None:
                fabric_workspace_obj._set_semanticmodel_connections(item_guid=item_guid, metadata_body=foundconn)
                # Set refresh schedule
                refreshschedule = fabric_workspace_obj._get_refreshschedule(item_name=item_name)
                if refreshschedule:
                    fabric_workspace_obj._update_refreshschedule(
                        item_guid=item_guid, metadata_body=refreshschedule.copy()
                    )
                if fabric_workspace_obj._get_isrefreshable(item_guid=item_guid):
                    refresh_dataset(fabric_workspace_obj=fabric_workspace_obj, item_guid=item_guid)


def find_datasource_connections(gateways: dict, gatewayconn: dict, datasetconn: dict) -> str:
    # Search for the connections on the gateways.
    # Search the gateways in the order they are listed in the parameter file
    foundconn = []
    gatewayObjectId = None

    for gatewayname in gateways:
        for dsconn in datasetconn:
            datasourceId = None
            gatewayId = None
            for gwconn in gatewayconn:
                # skip connections on other gateways
                if gwconn["clusterName"] == gatewayname:
                    # Match found on this gateway
                    if dsconn["connectionDetails"] == gwconn["connectionDetails"]:
                        datasourceId = gwconn["id"]
                        gatewayId = gwconn["clusterId"]
                        # Stop looking for this connection on this gateway and proceed with the next dataset connection
                        break
                    continue

            if datasourceId:
                foundconn.append(datasourceId)
                gatewayObjectId = gatewayId
            else:
                # This connection is not found on this gateway. Proceed with next gateway
                foundconn = []
                gatewayObjectId = None
                break
        if foundconn:
            # all connections found on a single gateway
            returnobject = {}
            returnobject["gatewayObjectId"] = gatewayObjectId
            returnobject["datasourceObjectIds"] = foundconn
            # return json.dumps(returnobject, indent=2)
            return returnobject
    logger.error("Not all gateway connections found!")
    return None


def refresh_dataset(fabric_workspace_obj: FabricWorkspace, item_guid: str):
    if "dataset_refresh_norefresh" in constants.FEATURE_FLAG:
        logger.info("Skipping Semantic model data refresh")
        return
    # refresh the dataset.
    # First get a list of refreshes and check if there is no refresh running

    max_retries = 10
    if "dataset_refresh_nowait" in constants.FEATURE_FLAG:
        max_retries = 1

    fabric_workspace_obj._invoke_refresh(
        item_guid=item_guid,
        max_retries=max_retries,
    )
