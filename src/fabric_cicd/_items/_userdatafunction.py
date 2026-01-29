# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy User Data Function item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class UserDataFunctionPublisher(ItemPublisher):
    """Publisher for User Data Function items."""

    item_type = ItemType.USER_DATA_FUNCTION.value
