# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Reflex item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class ActivatorPublisher(ItemPublisher):
    """Publisher for Reflex AKA Activator items."""

    item_type = ItemType.REFLEX.value
