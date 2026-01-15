# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy ML Experiment item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)


def publish_mlexperiments(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all experiment items from the repository.
    Only Publishes the Shell item, as only type and name are supported for MLExperiment items through api.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "MLExperiment"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type)


class MLExperimentPublisher(ItemPublisher):
    """Publisher for ML Experiment items."""

    def publish_all(self) -> None:
        """Publish all ML Experiment items."""
        publish_mlexperiments(self.fabric_workspace_obj)
