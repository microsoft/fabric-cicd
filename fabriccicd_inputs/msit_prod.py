"""MSIT PROD workspace definition.

Inherits the full item set from ``msit_dev`` and overrides only env-specific
fields. See ``msit_test.py`` for the same pattern.
"""

from dataclasses import replace

from ._schema import TargetEnvironment
from .msit_dev import WORKSPACE as DEV_WORKSPACE

WORKSPACE = replace(
    DEV_WORKSPACE,
    target=TargetEnvironment.PROD,
    workspace_name="",  # resolve to "fabric-cicd-prod" (or personal via FABRICCICD_USER)
    workspace_id="404e3248-3895-4834-bb58-b098491f6680",
)
