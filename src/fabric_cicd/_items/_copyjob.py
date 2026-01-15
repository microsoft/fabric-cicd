# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Copy Job item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class CopyJobPublisher(ItemPublisher):
    """Publisher for Copy Job items."""

    item_type = ItemType.COPY_JOB.value
