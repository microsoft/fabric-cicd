# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Ontology item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class OntologyPublisher(ItemPublisher):
    """Publisher for Ontology items."""

    item_type = ItemType.ONTOLOGY.value
