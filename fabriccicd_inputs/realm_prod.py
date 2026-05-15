"""Realm PROD workspace definition - all values inlined (no shared/common module)."""

from ._schema import (
    FabricCapacity,
    ProjectMetadata,
    SourceControlProvider,
    SourceControlSettings,
    TargetEnvironment,
    WorkspaceEnvironment,
)

WORKSPACE = WorkspaceEnvironment(
    target=TargetEnvironment.PROD,
    workspace_prefix="fabric-cicd",
    workspace_name="",
    workspace_id="",
    capacity=FabricCapacity(
        capacity_id="5b1574b7-0f67-4344-95e9-78fa4a5ef2ab",
        label="Realm (daily)",
    ),
    metadata=ProjectMetadata(
        owner="Core",
        team="DnA",
        tenant_id="33e01921-4d64-4f8c-a055-5bdaffd5e33d",
        project_name="fabric-cicd",
    ),
    repo_path=r"c:\Users\v-vijareddy\Asimov-vNext-Deployment\fabric",
    source_control=SourceControlSettings(
        provider=SourceControlProvider.AzureDevOps,
        organization="msazure",
        project="One",
        repository="Asimov-vNext-Deployment",
        branch="main",
        directory="fabric",
    ),
    access_control=[],
    lakehouses=[],
    spark_environments=[],
    spark_job_definitions=[],
    pipelines=[],
    api_base="https://dailyapi.powerbi.com/v1",
    realm_mode=True,
    realm_id="3e7a0129-b17c-4bfd-8e4a-97ab9448d806",
)
