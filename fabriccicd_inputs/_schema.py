"""Schema definitions: enums and dataclasses for the Fabric CI/CD topology."""

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


@dataclass
class ProjectMetadata:
    """Organizational metadata for the Fabric project."""

    owner: str
    team: str
    tenant_id: str
    project_name: str


@dataclass
class FabricCapacity:
    """A Fabric capacity (compute pool) referenced by workspaces."""

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
class PublishSettings:
    """Deployment behavior for ``publish_all_items``."""

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


@dataclass
class WorkspaceEnvironment:
    """A single workspace stage (DEV/TEST/PROD) with its dependencies.

    ``workspace_id`` may be left empty on a clean slate. The driver will
    create the workspace by name (``<prefix>-<target>``) and persist the
    new GUID back into the per-env input file.
    """

    target: TargetEnvironment
    capacity: FabricCapacity
    workspace_id: str = ""
    access_control: list[Identity] = field(default_factory=list)
    lakehouses: list[LakehouseDefinition] = field(default_factory=list)
    publish: PublishSettings = field(default_factory=PublishSettings)


@dataclass
class FabricTopology:
    """Top-level configuration tying project, repo, and workspaces together."""

    metadata: ProjectMetadata
    source_control: SourceControlSettings
    repo_path: str
    workspace_prefix: str
    workspaces: dict[str, WorkspaceEnvironment] = field(default_factory=dict)
    realm_mode: bool = False
    realm_id: str = ""
    api_base: str = "https://api.fabric.microsoft.com/v1"
