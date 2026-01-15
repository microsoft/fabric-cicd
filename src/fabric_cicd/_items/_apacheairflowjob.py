# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Apache Airflow Job item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher

logger = logging.getLogger(__name__)



class ApacheAirflowJobPublisher(ItemPublisher):
    """Publisher for Apache Airflow Job items."""

    item_type = "ApacheAirflowJob"

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a single Apache Airflow Job item."""
        self.fabric_workspace_obj._publish_item(item_name=item_name, item_type=self.item_type)

    def publish_all(self) -> None:
        """Publish all Apache Airflow Job items."""
        for item_name, item in self.fabric_workspace_obj.repository_items.get(self.item_type, {}).items():
            self.publish_one(item_name, item)
