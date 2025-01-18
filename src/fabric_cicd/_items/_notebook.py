# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging

"""
Functions to process and deploy Notebook item.
"""

logger = logging.getLogger(__name__)


def publish_notebooks(fabric_workspace_obj):
    """Publishes all notebook items from the repository."""
    item_type = "Notebook"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(
            item_name=item_name, item_type=item_type, func_process_file=_process_notebook_file_contents
        )


def _process_notebook_file_contents(fabric_workspace_obj, item_file_obj):
    if item_file_obj.name.startswith("notebook-content"):
        default_workspace_string = '"workspaceId": "00000000-0000-0000-0000-000000000000"'
        target_workspace_string = f'"workspaceId": "{fabric_workspace_obj.workspace_id}"'
        item_file_obj.contents = item_file_obj.contents.replace(default_workspace_string, target_workspace_string)

    return item_file_obj
