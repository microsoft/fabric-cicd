# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Notebook item."""

import logging
from multiprocessing.pool import ThreadPool

from fabric_cicd import FabricWorkspace

logger = logging.getLogger(__name__)


def publish_notebooks(fabric_workspace_obj: FabricWorkspace) -> None:
    """
    Publishes all notebook items from the repository.

    Args:
        fabric_workspace_obj: The FabricWorkspace object containing the items to be published.
    """
    item_type = "Notebook"

    with ThreadPool(fabric_workspace_obj.max_parallel_requests) as pool:
        pool.map(
            lambda item_name: fabric_workspace_obj._publish_item(item_name=item_name, item_type=item_type),
            fabric_workspace_obj.repository_items.get(item_type, {}),
        )
