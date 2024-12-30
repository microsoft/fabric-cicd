"""
Functions to process and deploy Notebook item.
"""


def _publish_notebooks(fabric_workspace_obj):
    """
    Publishes all notebook items from the repository.
    """
    item_type = "Notebook"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj.publish_item(item_name=item_name, item_type=item_type)
