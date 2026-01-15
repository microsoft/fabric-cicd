# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy GraphQL API item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class GraphQLApiPublisher(ItemPublisher):
    """Publisher for GraphQL API items."""

    item_type = ItemType.GRAPHQL_API.value
