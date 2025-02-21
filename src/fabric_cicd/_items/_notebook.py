# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Notebook item."""

import logging

from fabric_cicd import FabricWorkspace
from fabric_cicd._common._file import File
from fabric_cicd._common._item import Item

logger = logging.getLogger(__name__)


def publish_notebooks(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all notebook items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "Notebook"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=item_type, func_process_file=func_process_file
        )


def func_process_file(workspace_obj: FabricWorkspace, item_obj: Item, file_obj: File) -> str:
    """
    Custom file processing for notebook items.

    Args:
        workspace_obj: The FabricWorkspace object.
        item_obj: The item object.
        file_obj: The file object.
    """
    return workspace_obj._replace_workspace_ids(file_obj.contents, item_obj.type)
