# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Org App Audience item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class OrgAppAudiencePublisher(ItemPublisher):
    """Publisher for Org App Audience items."""

    item_type = ItemType.ORG_APP_AUDIENCE.value
