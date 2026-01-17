# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Notebook item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class NotebookPublisher(ItemPublisher):
    """Publisher for Notebook items."""

    item_type = ItemType.NOTEBOOK.value
