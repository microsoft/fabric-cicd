"""Shared values used across multiple environments (project, repo, capacities, security groups)."""

import os
from pathlib import Path

from ._schema import (
    FabricCapacity,
    Identity,
    IdentityKind,
    ProjectMetadata,
    SourceControlProvider,
    SourceControlSettings,
    WorkspaceRole,
)

# Local filesystem path to the cloned 'fabric' folder of the source repo.
# Override with the FABRIC_REPO_PATH environment variable, otherwise defaults
# to ../Asimov-vNext-Deployment/fabric relative to this driver script.
_DEFAULT_REPO_PATH = Path(__file__).resolve().parent.parent.parent / "Asimov-vNext-Deployment" / "fabric"
REPO_PATH = os.environ.get("FABRIC_REPO_PATH", str(_DEFAULT_REPO_PATH))

# Workspace name prefix (e.g. <prefix>-dev, <prefix>-test, <prefix>-prod).
WORKSPACE_PREFIX = os.environ.get("FABRIC_WORKSPACE_PREFIX", "fabric-cicd")

PROJECT = ProjectMetadata(
    owner="Core",
    team="DnA",
    tenant_id="33e01921-4d64-4f8c-a055-5bdaffd5e33d",
    project_name="fabric-cicd",
)

REPO = SourceControlSettings(
    provider=SourceControlProvider.AzureDevOps,
    organization="msazure",
    project="One",
    repository="Asimov-vNext-Deployment",
    branch="dev",
    directory="fabric",
)

MSIT_CAPACITY = FabricCapacity(
    capacity_id="F41BC187-38C5-4835-817C-629BD784ADD7",
    label="MSIT",
)

REALM_CAPACITY = FabricCapacity(
    capacity_id="5b1574b7-0f67-4344-95e9-78fa4a5ef2ab",
    label="Realm (daily)",
)

REALM_ID = "3e7a0129-b17c-4bfd-8e4a-97ab9448d806"
REALM_API_BASE = "https://dailyapi.powerbi.com/v1"

SECURITY_GROUP = Identity(
    display_name="fabric-cicd-contributors",
    object_id="cbb157e6-143f-4eb7-a9fb-688199a3b569",
    kind=IdentityKind.Group,
    workspace_role=WorkspaceRole.Contributor,
)

# Lakehouse names provisioned in every environment of the MSIT topology.
# Per-environment access lists are defined in each msit_<env>.py module.
LAKEHOUSE_NAMES = ["Lakehouse1", "Lakehouse2", "Lakehouse3", "Lakehouse4", "Lakehouse5"]
