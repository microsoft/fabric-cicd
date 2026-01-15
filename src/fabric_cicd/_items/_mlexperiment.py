# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy ML Experiment item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class MLExperimentPublisher(ItemPublisher):
    """Publisher for ML Experiment items."""

    item_type = ItemType.ML_EXPERIMENT.value
