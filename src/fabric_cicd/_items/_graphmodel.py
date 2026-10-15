# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Graph Model item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class GraphModelPublisher(ItemPublisher):
    """Publisher for Graph Model items."""

    item_type = ItemType.GRAPH_MODEL.value
