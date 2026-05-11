"""MSIT TEST workspace definition."""

from ._common import LAKEHOUSE_NAMES, MSIT_CAPACITY, SECURITY_GROUP
from ._schema import LakehouseDefinition, TargetEnvironment, WorkspaceEnvironment

# Per-environment access for TEST. Add DataAccessEntry/TableAccessEntry items
# inside each LakehouseDefinition as access is granted.
LAKEHOUSES = [LakehouseDefinition(name=name) for name in LAKEHOUSE_NAMES]

WORKSPACE = WorkspaceEnvironment(
    target=TargetEnvironment.TEST,
    workspace_id="",
    capacity=MSIT_CAPACITY,
    access_control=[SECURITY_GROUP],
    lakehouses=LAKEHOUSES,
)
