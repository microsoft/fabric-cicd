# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Data Build Tool Job item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class DataBuildToolJobPublisher(ItemPublisher):
    """Publisher for Data Build Tool Job items."""

    item_type = ItemType.DATA_BUILD_TOOL_JOB.value
