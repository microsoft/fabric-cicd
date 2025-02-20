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
            item_name=item_name, item_type=item_type, func_process_file=func_process_file
        )


def func_process_file(workspace_obj, item_obj, file_obj):
    """Custom file processing for notebook items."""
    return workspace_obj._replace_workspace_ids(file_obj.contents, item_obj.type)
