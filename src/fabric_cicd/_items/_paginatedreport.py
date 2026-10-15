# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy PaginatedReport item."""

from fabric_cicd._items._base_publisher import ItemPublisher, ParallelConfig
from fabric_cicd.constants import ItemType


class PaginatedReportPublisher(ItemPublisher):
    """Publisher for PaginatedReport items."""

    item_type = ItemType.PAGINATED_REPORT.value
    parallel_config = ParallelConfig(enabled=False)
