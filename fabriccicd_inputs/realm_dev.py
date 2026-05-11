"""Realm DEV workspace definition."""

from ._common import REALM_CAPACITY
from ._schema import TargetEnvironment, WorkspaceEnvironment

LAKEHOUSES = []

WORKSPACE = WorkspaceEnvironment(
    target=TargetEnvironment.DEV,
    workspace_id="",
    capacity=REALM_CAPACITY,
    access_control=[],
    lakehouses=LAKEHOUSES,
)
