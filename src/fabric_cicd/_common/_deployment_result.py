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
    """Structured result of a config-based deployment operation.

    Returned by ``deploy_with_config`` on success. On failure, an instance is
    attached to the raised exception as ``e.deployment_result`` with ``status``
    set to ``DeploymentStatus.FAILED``.

    Attributes:
        status: The deployment status.
        message: A human-readable message describing the result.
        responses: Optional dictionary of API response data from the deployment.
    """

    status: DeploymentStatus
    message: str
    responses: Optional[dict] = field(default=None)
