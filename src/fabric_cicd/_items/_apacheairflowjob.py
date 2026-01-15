# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy Apache Airflow Job item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class ApacheAirflowJobPublisher(ItemPublisher):
    """Publisher for Apache Airflow Job items."""

    item_type = ItemType.APACHE_AIRFLOW_JOB.value
