# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Notebook item."""

from fabric_cicd._common._item import Item
from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import API_FORMAT_MAPPING, ItemType


class NotebookPublisher(ItemPublisher):
    """Publisher for Notebook items."""

    item_type = ItemType.NOTEBOOK.value

    def publish_one(self, item_name: str, item: Item) -> None:
        """Publish a Notebook item."""
        is_ipynb = any(file.file_path.suffix == ".ipynb" for file in item.item_files)

        kwargs = {}
        if is_ipynb:
            api_format = API_FORMAT_MAPPING.get(self.item_type)
            if api_format:
                kwargs["api_format"] = api_format

        self.fabric_workspace_obj._publish_item(
            item_name=item_name,
            item_type=self.item_type,
            **kwargs,
        )
