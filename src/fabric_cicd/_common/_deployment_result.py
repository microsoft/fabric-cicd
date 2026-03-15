# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Deployment result types for config-based deployment operations."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class DeploymentStatus(str, Enum):
    """Enumeration of deployment status values for deploy_with_config results."""

    COMPLETED = "completed"
    """Deployment completed successfully without any errors."""
    FAILED = "failed"
    """Deployment failed due to one or more errors."""


@dataclass
class DeploymentResult:
    """Result of a config-based deployment operation.

    This class provides a structured way to return deployment results.
    Currently only returned on successful completion; failures raise exceptions
    with ``deployment_status`` (set to ``DeploymentStatus.FAILED``) and
    ``deployment_message`` attributes attached.

    Attributes:
        status: The deployment status (DeploymentStatus.COMPLETED on success).
        message: A human-readable message describing the result.
        responses: Optional dictionary of API response data from the deployment.
    """

    status: DeploymentStatus
    message: str
    responses: Optional[dict] = field(default=None)
