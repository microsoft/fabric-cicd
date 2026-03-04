# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Deployment result types for config-based deployment operations."""

from dataclasses import dataclass
from enum import Enum


class DeploymentStatus(str, Enum):
    """Enumeration of deployment status values for deploy_with_config results."""

    COMPLETED = "completed"
    """Deployment completed successfully without any errors."""


@dataclass
class DeploymentResult:
    """Result of a config-based deployment operation.

    This class provides a structured way to return deployment results.
    Currently only returned on successful completion; failures raise exceptions.

    Attributes:
        status: The deployment status (DeploymentStatus.COMPLETED on success).
        message: A human-readable message describing the result.
    """

    status: DeploymentStatus
    message: str
