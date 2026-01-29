# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Eventstream item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class EventstreamPublisher(ItemPublisher):
    """Publisher for Eventstream items."""

    item_type = ItemType.EVENTSTREAM.value
