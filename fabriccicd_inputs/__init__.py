"""
Fabric CI/CD typed input package.

Per-environment workspace definitions live in the ``msit_*.py`` and ``realm_*.py``
modules. Shared values (project, repo, capacities, security groups) are in
``_common.py``. Schemas (enums and dataclasses) are in ``_schema.py``.

Top-level topologies are assembled here:

    from fabriccicd_inputs import MSIT_TOPOLOGY, REALM_TOPOLOGY, get_workspace
"""

from . import (
    msit_dev,
    msit_prod,
    msit_test,
    realm_dev,
    realm_prod,
    realm_test,
)
from ._common import (
    MSIT_CAPACITY,
    PROJECT,
    REALM_API_BASE,
    REALM_CAPACITY,
    REALM_ID,
    REPO,
    REPO_PATH,
    SECURITY_GROUP,
    WORKSPACE_PREFIX,
)
from ._schema import (
    DataAccessEntry,
    DataPermission,
    FabricCapacity,
    FabricTopology,
    FileAccessEntry,
    Identity,
    IdentityKind,
    LakehouseDefinition,
    ProjectMetadata,
    PublishSettings,
    SourceControlProvider,
    SourceControlSettings,
    TableAccessEntry,
    TargetEnvironment,
    WorkspaceEnvironment,
    WorkspaceRole,
)

MSIT_TOPOLOGY = FabricTopology(
    metadata=PROJECT,
    source_control=REPO,
    repo_path=REPO_PATH,
    workspace_prefix=WORKSPACE_PREFIX,
    workspaces={
        "DEV": msit_dev.WORKSPACE,
        "TEST": msit_test.WORKSPACE,
        "PROD": msit_prod.WORKSPACE,
    },
)

REALM_TOPOLOGY = FabricTopology(
    metadata=PROJECT,
    source_control=REPO,
    repo_path=REPO_PATH,
    workspace_prefix=WORKSPACE_PREFIX,
    workspaces={
        "DEV": realm_dev.WORKSPACE,
        "TEST": realm_test.WORKSPACE,
        "PROD": realm_prod.WORKSPACE,
    },
    realm_mode=True,
    realm_id=REALM_ID,
    api_base=REALM_API_BASE,
)

TOPOLOGIES: dict[str, FabricTopology] = {
    "msit": MSIT_TOPOLOGY,
    "realm": REALM_TOPOLOGY,
}


def get_topology(realm_mode: bool = False) -> FabricTopology:
    """Return the MSIT or Realm topology based on the mode flag."""
    return REALM_TOPOLOGY if realm_mode else MSIT_TOPOLOGY


def get_workspace(env: str, realm_mode: bool = False) -> WorkspaceEnvironment:
    """Return the workspace environment for the given target (DEV/TEST/PROD)."""
    topology = get_topology(realm_mode)
    key = env.upper()
    if key not in topology.workspaces:
        valid = ", ".join(topology.workspaces.keys())
        msg = f"Unknown environment '{env}'. Valid values: {valid}"
        raise ValueError(msg)
    return topology.workspaces[key]


def _print_summary() -> None:
    """Print a human-readable summary of the configured topologies."""
    for mode, topology in TOPOLOGIES.items():
        print(f"\n=== {mode.upper()} TOPOLOGY ===")
        print(f"  Project       : {topology.metadata.project_name} ({topology.metadata.team})")
        print(f"  Tenant        : {topology.metadata.tenant_id}")
        print(f"  API base      : {topology.api_base}")
        print(f"  Repo path     : {topology.repo_path}")
        print(f"  Prefix        : {topology.workspace_prefix}")
        print(
            f"  Source control: {topology.source_control.provider.value} "
            f"{topology.source_control.organization}/{topology.source_control.project}/"
            f"{topology.source_control.repository}@{topology.source_control.branch}"
        )
        if topology.realm_mode:
            print(f"  Realm ID      : {topology.realm_id}")
        for env, ws in topology.workspaces.items():
            print(f"  [{env}] {ws.workspace_id}  capacity={ws.capacity.capacity_id}")
            for ident in ws.access_control:
                print(f"        access: {ident.kind.value} '{ident.display_name}' -> {ident.workspace_role.value}")
            for lh in ws.lakehouses:
                emails = ", ".join(entry.email for entry in lh.access_list) or "(none)"
                print(f"        lakehouse: {lh.name} -> {emails}")


__all__ = [
    "MSIT_CAPACITY",
    "MSIT_TOPOLOGY",
    "PROJECT",
    "REALM_API_BASE",
    "REALM_CAPACITY",
    "REALM_ID",
    "REALM_TOPOLOGY",
    "REPO",
    "REPO_PATH",
    "SECURITY_GROUP",
    "TOPOLOGIES",
    "WORKSPACE_PREFIX",
    "DataAccessEntry",
    "DataPermission",
    "FabricCapacity",
    "FabricTopology",
    "FileAccessEntry",
    "Identity",
    "IdentityKind",
    "LakehouseDefinition",
    "ProjectMetadata",
    "PublishSettings",
    "SourceControlProvider",
    "SourceControlSettings",
    "TableAccessEntry",
    "TargetEnvironment",
    "WorkspaceEnvironment",
    "WorkspaceRole",
    "get_topology",
    "get_workspace",
]


if __name__ == "__main__":
    _print_summary()
