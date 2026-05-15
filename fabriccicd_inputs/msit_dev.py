"""MSIT DEV workspace definition - all values inlined (no shared/common module)."""

from ._schema import (
    DataAccessEntry,
    DataPermission,
    FabricCapacity,
    FileAccessEntry,
    Identity,
    IdentityKind,
    LakehouseDefinition,
    ProjectMetadata,
    SourceControlProvider,
    SourceControlSettings,
    TableAccessEntry,
    TargetEnvironment,
    WorkspaceEnvironment,
    WorkspaceRole,
)

WORKSPACE = WorkspaceEnvironment(
    target=TargetEnvironment.DEV,
    workspace_prefix="fabric-cicd",
    workspace_name="",  # leave empty to resolve to "fabric-cicd-dev"
    workspace_id="34831a7c-bee0-4089-8d68-f1c0524bcb1d",
    capacity=FabricCapacity(
        capacity_id="F41BC187-38C5-4835-817C-629BD784ADD7",
        label="MSIT",
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
        branch="dev",
        directory="fabric",
    ),
    access_control=[
        Identity(
            display_name="fabric-cicd-contributors",
            object_id="cbb157e6-143f-4eb7-a9fb-688199a3b569",
            kind=IdentityKind.Group,
            workspace_role=WorkspaceRole.Contributor,
        ),
    ],
    lakehouses=[
        LakehouseDefinition(
            name="Lakehouse1",
            access_list=[
                DataAccessEntry(
                    display_name="Sikana.Tanupabrungsun",
                    email="Sikana.Tanupabrungsun@microsoft.com",
                    permission=DataPermission.ReadWrite,
                ),
            ],
            table_access=[
                TableAccessEntry(
                    display_name="Sikana.Tanupabrungsun",
                    email="Sikana.Tanupabrungsun@microsoft.com",
                    tables=["customers"],
                    permission=DataPermission.ReadWrite,
                ),
                TableAccessEntry(
                    display_name="v-vijareddy",
                    email="v-vijareddy@microsoft.com",
                    tables=["orders"],
                    permission=DataPermission.ReadWrite,
                ),
            ],
            file_access=[
                FileAccessEntry(
                    display_name="Sikana.Tanupabrungsun",
                    email="Sikana.Tanupabrungsun@microsoft.com",
                    paths=["/Files/raw/customers.csv"],
                    permission=DataPermission.ReadWrite,
                ),
                FileAccessEntry(
                    display_name="v-vijareddy",
                    email="v-vijareddy@microsoft.com",
                    paths=["/Files/raw/orders.csv"],
                    permission=DataPermission.ReadWrite,
                ),
            ],
        ),
        LakehouseDefinition(
            name="Lakehouse2",
            access_list=[
                DataAccessEntry(
                    display_name="v-vijareddy",
                    email="v-vijareddy@microsoft.com",
                    permission=DataPermission.ReadWrite,
                ),
            ],
            file_access=[
                FileAccessEntry(
                    display_name="v-vijareddy",
                    email="v-vijareddy@microsoft.com",
                    paths=["/Files/raw"],
                    permission=DataPermission.ReadWrite,
                ),
            ],
        ),
        LakehouseDefinition(
            name="Lakehouse3",
            access_list=[
                DataAccessEntry(
                    display_name="UserC",
                    email="userC@domain.com",
                    permission=DataPermission.ReadWrite,
                ),
            ],
        ),
        LakehouseDefinition(
            name="Lakehouse4",
            access_list=[
                DataAccessEntry(
                    display_name="UserD",
                    email="userD@domain.com",
                    permission=DataPermission.ReadWrite,
                ),
            ],
        ),
        LakehouseDefinition(
            name="Lakehouse5",
            access_list=[
                DataAccessEntry(
                    display_name="UserE",
                    email="userE@domain.com",
                    permission=DataPermission.ReadWrite,
                ),
            ],
        ),
    ],
    spark_environments=[
        # Example - uncomment and customize per env.
        # SparkEnvironment(
        #     name="DevSparkEnv",
        #     runtime_version="1.3",
        #     spark_properties={"spark.sql.shuffle.partitions": "200"},
        #     libraries=[SparkLibrary(file_name="my_utils-0.1.0-py3-none-any.whl")],
        # ),
    ],
    spark_job_definitions=[
        # Example - uncomment and customize per env.
        # SparkJobDefinition(
        #     name="MyJob",
        #     language=SparkLanguage.Python,
        #     executable_file="main.py",
        #     command_line_arguments="arg1 true",
        #     additional_library_uris=["pipeline_config.py"],
        #     default_lakehouse="Lakehouse1",
        #     environment="DevSparkEnv",
        # ),
    ],
    pipelines=[
        # Example - uncomment and customize per env.
        # Pipeline(
        #     name="MyPipeline",
        #     activities=[],
        # ),
    ],
    api_base="https://api.fabric.microsoft.com/v1",
    realm_mode=False,
    realm_id="",
)
