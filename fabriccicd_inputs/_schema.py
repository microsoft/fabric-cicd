"""Schema definitions: enums and dataclasses for the Fabric CI/CD inputs.

Each environment file declares its own ``WorkspaceEnvironment`` with all values
inlined (capacity, source-control, repo path, lakehouses, SJDs, Spark envs,
pipelines, etc.). There is no shared/common module - everything is explicit
per env.
"""

from dataclasses import dataclass, field
from enum import Enum


class TargetEnvironment(str, Enum):
    """Deployment stage for a Fabric workspace."""

    DEV = "DEV"
    TEST = "TEST"
    PROD = "PROD"


class WorkspaceRole(str, Enum):
    """Fabric workspace permission level for a security principal."""

    Admin = "Admin"
    Member = "Member"
    Contributor = "Contributor"
    Viewer = "Viewer"


class DataPermission(str, Enum):
    """Lakehouse data-access permission level."""

    ReadWrite = "ReadWrite"
    Read = "Read"


class IdentityKind(str, Enum):
    """Type of Microsoft Entra security principal."""

    User = "User"
    Group = "Group"
    ServicePrincipal = "ServicePrincipal"


class SourceControlProvider(str, Enum):
    """Git provider supported by Fabric source control."""

    AzureDevOps = "AzureDevOps"
    GitHub = "GitHub"


class SparkLanguage(str, Enum):
    """Supported Spark Job Definition languages."""

    Python = "Python"
    Scala = "Scala"
    Java = "Java"
    R = "R"


@dataclass
class FabricCapacity:
    """A Fabric capacity (compute pool) referenced by a workspace."""

    capacity_id: str
    sku: str = ""
    label: str = ""


@dataclass
class Identity:
    """A security principal that holds a workspace role."""

    display_name: str
    object_id: str
    kind: IdentityKind
    workspace_role: WorkspaceRole
    email: str = ""


@dataclass
class DataAccessEntry:
    """A user-level data permission grant on a lakehouse."""

    display_name: str
    email: str
    permission: DataPermission = DataPermission.ReadWrite
    object_id: str = ""


@dataclass
class TableAccessEntry:
    """A user-level data permission grant scoped to specific tables in a lakehouse."""

    display_name: str
    email: str
    tables: list[str] = field(default_factory=list)
    permission: DataPermission = DataPermission.ReadWrite
    object_id: str = ""


@dataclass
class FileAccessEntry:
    """A user-level data permission grant scoped to specific files/folders under Files/ in a lakehouse."""

    display_name: str
    email: str
    paths: list[str] = field(default_factory=list)
    permission: DataPermission = DataPermission.ReadWrite
    object_id: str = ""


@dataclass
class LakehouseDefinition:
    """Definition of a lakehouse to provision and its access list."""

    name: str
    label: str = ""
    use_schemas: bool = False
    access_list: list[DataAccessEntry] = field(default_factory=list)
    table_access: list[TableAccessEntry] = field(default_factory=list)
    file_access: list[FileAccessEntry] = field(default_factory=list)


@dataclass
class SourceControlSettings:
    """Settings used to connect a workspace to a Git repository."""

    provider: SourceControlProvider
    organization: str
    project: str
    repository: str
    branch: str
    directory: str


@dataclass
class SparkLibrary:
    """A custom library attached to a Spark environment."""

    file_name: str
    """Library file name as committed under the env's CustomLibraries/ folder (e.g. ``my_utils-0.1.0-py3-none-any.whl``)."""

    library_type: str = "PythonWheel"
    """``PythonWheel``, ``Jar``, or ``CondaYml``."""


@dataclass
class SparkPoolConfig:
    """Optional Spark pool / compute config for a Spark environment."""

    name: str = ""
    node_family: str = ""
    node_size: str = ""
    auto_scale_enabled: bool = True
    min_node_count: int = 1
    max_node_count: int = 4
    dynamic_executor_allocation: bool = True


@dataclass
class SparkEnvironment:
    """Definition of a Fabric Spark Environment item.

    The driver generates ``<name>.Environment/.platform`` plus
    ``Setting/Sparkcompute.yml`` and ``Libraries/`` under the env's repo path
    before deployment.
    """

    name: str
    runtime_version: str = "1.3"
    spark_properties: dict[str, str] = field(default_factory=dict)
    libraries: list[SparkLibrary] = field(default_factory=list)
    pool: SparkPoolConfig = field(default_factory=SparkPoolConfig)
    description: str = ""


@dataclass
class SparkJobDefinition:
    """Definition of a Fabric Spark Job Definition item.

    The driver generates ``<name>.SparkJobDefinition/.platform`` plus
    ``SparkJobDefinitionV1.json`` under the env's repo path before deployment.
    The actual source files referenced by ``executable_file`` and
    ``additional_library_uris`` must already exist under ``Main/`` and
    ``Libs/`` in the generated folder (committed in the repo by the user).
    """

    name: str
    language: SparkLanguage = SparkLanguage.Python
    executable_file: str = "main.py"
    """File name under ``Main/`` (e.g. ``main.py`` or ``myapp.jar``)."""

    main_class: str = ""
    """Entry class for JVM jobs (Scala/Java). Leave empty for Python/R."""

    command_line_arguments: str = ""
    additional_library_uris: list[str] = field(default_factory=list)
    """File names under ``Libs/``."""

    default_lakehouse: str = ""
    """Name of a lakehouse declared in this same env. Resolved to ID at deploy time."""

    additional_lakehouses: list[str] = field(default_factory=list)
    environment: str = ""
    """Name of a Spark environment declared in this same env. Resolved to ID at deploy time."""

    retry_policy: dict | None = None


@dataclass
class PipelineActivity:
    """A single activity inside a Data Pipeline."""

    name: str
    type: str
    """Fabric activity type (e.g. ``Copy``, ``ExecuteNotebook``, ``SparkJobDefinition``)."""

    type_properties: dict = field(default_factory=dict)
    depends_on: list[dict] = field(default_factory=list)
    policy: dict = field(default_factory=dict)
    user_properties: list[dict] = field(default_factory=list)


@dataclass
class Pipeline:
    """Definition of a Fabric Data Pipeline item.

    The driver generates ``<name>.DataPipeline/.platform`` plus
    ``pipeline-content.json`` under the env's repo path before deployment.
    """

    name: str
    description: str = ""
    activities: list[PipelineActivity] = field(default_factory=list)
    parameters: dict = field(default_factory=dict)
    variables: dict = field(default_factory=dict)
    annotations: list[str] = field(default_factory=list)


@dataclass
class ProjectMetadata:
    """Organizational metadata for the Fabric project (per env)."""

    owner: str
    team: str
    tenant_id: str
    project_name: str


@dataclass
class WorkspaceEnvironment:
    """A single workspace stage (DEV/TEST/PROD) with all values inlined.

    Every field is declared explicitly per env - no shared/common module.

    Workspace display name resolution:
      - If ``workspace_name`` is set, use it as-is.
      - Otherwise, the name is ``f"{workspace_prefix}-{target.value.lower()}"``.

    ``workspace_id`` may be empty on a clean slate. The driver will create the
    workspace by display name and persist the new GUID back into this file.
    """

    target: TargetEnvironment
    workspace_prefix: str
    """Required. Final display name is ``<workspace_prefix>-<env>`` unless overridden."""

    capacity: FabricCapacity
    metadata: ProjectMetadata
    repo_path: str
    source_control: SourceControlSettings

    workspace_name: str = ""
    """Optional. If set, overrides ``<workspace_prefix>-<env>``."""

    workspace_id: str = ""
    access_control: list[Identity] = field(default_factory=list)

    lakehouses: list[LakehouseDefinition] = field(default_factory=list)
    spark_environments: list[SparkEnvironment] = field(default_factory=list)
    spark_job_definitions: list[SparkJobDefinition] = field(default_factory=list)
    pipelines: list[Pipeline] = field(default_factory=list)

    item_types_in_scope: list[str] = field(
        default_factory=lambda: [
            "Notebook",
            "DataPipeline",
            "Environment",
            "Lakehouse",
            "SparkJobDefinition",
        ]
    )
    remove_orphans: bool = True
    parameter_file: str = "parameter.yml"

    # Fabric API targeting (per env so realm/dailyapi vs MSIT are explicit).
    api_base: str = "https://api.fabric.microsoft.com/v1"
    realm_mode: bool = False
    realm_id: str = ""

    def resolved_workspace_name(self) -> str:
        """Return the effective Fabric workspace display name for this env."""
        if self.workspace_name:
            return self.workspace_name
        return f"{self.workspace_prefix}-{self.target.value.lower()}"
