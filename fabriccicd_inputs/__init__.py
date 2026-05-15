"""
Fabric CI/CD typed input package.

Per-environment workspace definitions live in the ``msit_*.py`` and ``realm_*.py``
modules. Each module declares its own ``WORKSPACE`` (a ``WorkspaceEnvironment``)
with all values inlined - capacity, source control, repo path, lakehouses, Spark
job definitions, Spark environments, pipelines, etc. There is no shared common
module.

Resolve a workspace via ``get_workspace(env, realm_mode=False)``.
"""

from . import (
    msit_dev,
    msit_prod,
    msit_test,
    realm_dev,
    realm_prod,
    realm_test,
)
from ._schema import (
    DataAccessEntry,
    DataPermission,
    FabricCapacity,
    FileAccessEntry,
    Identity,
    IdentityKind,
    LakehouseDefinition,
    Pipeline,
    PipelineActivity,
    ProjectMetadata,
    SourceControlProvider,
    SourceControlSettings,
    SparkEnvironment,
    SparkJobDefinition,
    SparkLanguage,
    SparkLibrary,
    SparkPoolConfig,
    TableAccessEntry,
    TargetEnvironment,
    WorkspaceEnvironment,
    WorkspaceRole,
)

# Per-mode env tables. Each value is a fully self-contained WorkspaceEnvironment.
MSIT_WORKSPACES: dict[str, WorkspaceEnvironment] = {
    "DEV": msit_dev.WORKSPACE,
    "TEST": msit_test.WORKSPACE,
    "PROD": msit_prod.WORKSPACE,
}

REALM_WORKSPACES: dict[str, WorkspaceEnvironment] = {
    "DEV": realm_dev.WORKSPACE,
    "TEST": realm_test.WORKSPACE,
    "PROD": realm_prod.WORKSPACE,
}


def get_workspace(env: str, realm_mode: bool = False) -> WorkspaceEnvironment:
    """Return the ``WorkspaceEnvironment`` for the given target (DEV/TEST/PROD).

    ``realm_mode`` selects between the MSIT (default) and Realm input sets.
    """
    table = REALM_WORKSPACES if realm_mode else MSIT_WORKSPACES
    key = env.upper()
    if key not in table:
        valid = ", ".join(table.keys())
        msg = f"Unknown environment '{env}'. Valid values: {valid}"
        raise ValueError(msg)
    return table[key]


def _print_summary() -> None:
    """Print a human-readable summary of every configured workspace."""
    for mode_label, table in (("MSIT", MSIT_WORKSPACES), ("REALM", REALM_WORKSPACES)):
        print(f"\n=== {mode_label} WORKSPACES ===")
        for env, ws in table.items():
            print(f"  [{env}] {ws.resolved_workspace_name()}  id={ws.workspace_id or '(unset)'}")
            print(f"        project   : {ws.metadata.project_name} ({ws.metadata.team})")
            print(f"        tenant    : {ws.metadata.tenant_id}")
            print(f"        capacity  : {ws.capacity.capacity_id} ({ws.capacity.label})")
            print(f"        api_base  : {ws.api_base}  realm_mode={ws.realm_mode}")
            print(f"        repo_path : {ws.repo_path}")
            print(
                f"        git       : {ws.source_control.provider.value} "
                f"{ws.source_control.organization}/{ws.source_control.project}/"
                f"{ws.source_control.repository}@{ws.source_control.branch}"
            )
            for ident in ws.access_control:
                print(f"        access    : {ident.kind.value} '{ident.display_name}' -> {ident.workspace_role.value}")
            for lh in ws.lakehouses:
                emails = ", ".join(entry.email for entry in lh.access_list) or "(none)"
                print(f"        lakehouse : {lh.name} -> {emails}")
            for sjd in ws.spark_job_definitions:
                print(f"        SJD       : {sjd.name} ({sjd.language.value}) -> {sjd.executable_file}")
            for se in ws.spark_environments:
                print(f"        spark env : {se.name} (runtime {se.runtime_version})")
            for pl in ws.pipelines:
                print(f"        pipeline  : {pl.name} ({len(pl.activities)} activities)")


__all__ = [
    "MSIT_WORKSPACES",
    "REALM_WORKSPACES",
    "DataAccessEntry",
    "DataPermission",
    "FabricCapacity",
    "FileAccessEntry",
    "Identity",
    "IdentityKind",
    "LakehouseDefinition",
    "Pipeline",
    "PipelineActivity",
    "ProjectMetadata",
    "SourceControlProvider",
    "SourceControlSettings",
    "SparkEnvironment",
    "SparkJobDefinition",
    "SparkLanguage",
    "SparkLibrary",
    "SparkPoolConfig",
    "TableAccessEntry",
    "TargetEnvironment",
    "WorkspaceEnvironment",
    "WorkspaceRole",
    "get_workspace",
]


if __name__ == "__main__":
    _print_summary()
