# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Base interface for all item publishers."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fabric_cicd._common._item import Item
    from fabric_cicd.fabric_workspace import FabricWorkspace


class ItemPublisher(ABC):
    """Base interface for all item type publishers."""

    """
    Mandatory property to be set by each publisher.
    """
    item_type: str

    def __init__(self, fabric_workspace_obj: "FabricWorkspace") -> None:
        """
        Initialize the publisher with a FabricWorkspace object.

        Args:
            fabric_workspace_obj: The FabricWorkspace object containing items to be published.
        """
        self.fabric_workspace_obj = fabric_workspace_obj

    @abstractmethod
    def publish_one(self, item_name: str, item: "Item") -> None:
        """
        Publish a single item.

        Args:
            item_name: The name of the item to publish.
            item: The Item object to publish.

        This method must be implemented by each concrete item publisher.
        """

    @abstractmethod
    def publish_all(self) -> None:
        """
        Execute the publish operation for this item type.

        This method must be implemented by each concrete item publisher.
        """
        pass
