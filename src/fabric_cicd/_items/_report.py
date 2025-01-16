# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging

"""
Functions to process and deploy Smemantic Model item.
"""

logger = logging.getLogger(__name__)


def publish_reports(fabric_workspace_obj):
    """Publishes all semantic model items from the repository."""
    item_type = "Report"

    for item_name in fabric_workspace_obj.repository_items.get(item_type, {}):
        fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type, excluded_directories={".pbi"})


Need custom handling of pbir files. 

[ERROR] 18:27:41 - Operation failed. Error Code: UnknownError. Error Message: definition.pbir holds the relative path (ByPath) reference to the semantic model. Fabric REST API only supports byConnection references.

See C:\Users\jaknigh\Repositories\fabric-cicd-forked\fabric_cicd.error.log for full details.

https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-report#definitionpbir
