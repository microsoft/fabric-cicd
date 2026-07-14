<!--
  AUTO-GENERATED CANDIDATE — shared codebase reference for triage skills.

  This file is the SINGLE source of the "Codebase Reference" block. Skills under
  .github/prompts/skills/ include it (concatenated at workflow time) instead of each
  duplicating the reference.

  Phase 3 (optional): regenerate this file from src/fabric_cicd/constants.py at workflow
  start via `node .github/triage-tools/gen_context.mjs` so it can never drift. Until then
  keep it in sync manually with constants.py.
-->

# fabric-cicd Codebase Reference

Only reference API functions, parameters, item types, feature flags, environment
variables, and exceptions listed below. Do not invent or assume any capability not
documented here. If unsure, direct the user to the official docs.

## About the Library

- Python 3.9–3.13, pip-installable (`pip install fabric-cicd`).
- Programmatic API — not a CLI. Users write Python scripts that call library functions.
- Core workflow: initialize `FabricWorkspace` → call `publish_all_items()` /
  `unpublish_all_orphan_items()`, or use `deploy_with_config()` for YAML-based deployment.
- Authentication via an explicit `token_credential` parameter (any Azure `TokenCredential`).
- Full deployment model — deploys all in-scope items every time; no commit-diff logic by default.
- Items deploy in dependency order defined by `SERIAL_ITEM_PUBLISH_ORDER` in `constants.py`.
- Parameterization via `parameter.yml` (`find_replace`, `key_value_replace`, `spark_pool`,
  `semantic_model_binding`).
- Feature flags control experimental and destructive features.
- Repository directory follows `ItemName.ItemType/` folder convention with `.platform` metadata files.
- GitHub repo: https://github.com/microsoft/fabric-cicd
- Official docs: https://microsoft.github.io/fabric-cicd/latest/
- Fabric REST API docs: https://learn.microsoft.com/en-us/rest/api/fabric/

## Live Microsoft docs (authoritative for Fabric behavior — cite for API/platform questions)

- Fabric CI/CD (Microsoft Learn): https://learn.microsoft.com/en-us/fabric/cicd/
- Fabric REST API reference: https://learn.microsoft.com/en-us/rest/api/fabric/
- Item definition overview (definition payloads / formats): https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/item-definition-overview

## Documentation Pages (use for citations)

- PyPI: https://pypi.org/project/fabric-cicd/
- Getting started: https://microsoft.github.io/fabric-cicd/latest/how_to/getting_started/
- Supported item types: https://microsoft.github.io/fabric-cicd/latest/how_to/item_types/
- Parameterization: https://microsoft.github.io/fabric-cicd/latest/how_to/parameterization/
- Config deployment: https://microsoft.github.io/fabric-cicd/latest/how_to/config_deployment/
- Optional features / feature flags: https://microsoft.github.io/fabric-cicd/latest/how_to/optional_feature/
- Troubleshooting: https://microsoft.github.io/fabric-cicd/latest/how_to/troubleshooting/
- Authentication examples: https://microsoft.github.io/fabric-cicd/latest/example/authentication/
- Release pipeline examples: https://microsoft.github.io/fabric-cicd/latest/example/release_pipeline/
- Code reference (API docs): https://microsoft.github.io/fabric-cicd/latest/code_reference/
- Changelog: https://microsoft.github.io/fabric-cicd/latest/changelog/

## Public API (`__all__` in `src/fabric_cicd/__init__.py`)

| Symbol | Purpose |
|---|---|
| `FabricWorkspace(*, workspace_id, repository_directory, token_credential, ...)` | Initialize workspace connection. Keyword arguments required. Either `workspace_id` or `workspace_name` must be provided. |
| `publish_all_items(workspace, ...)` | Deploy all in-scope items to the target workspace. |
| `unpublish_all_orphan_items(workspace, ...)` | Remove deployed items not found in the repository. |
| `deploy_with_config(config_file_path, *, environment, token_credential, ...)` | Config-based deployment from a YAML file. |
| `append_feature_flag(flag)` | Enable a feature flag at runtime. |
| `change_log_level("DEBUG")` | Enable debug logging for troubleshooting. |
| `configure_external_file_logging(...)` | Configure external file logging. |
| `configure_fabric_fqdn(...)` | Override the Fabric API FQDN. |
| `disable_file_logging()` | Disable file-based logging. |
| `get_changed_items(repository_directory)` | Get list of git-changed items for selective deployment. |
| `DeploymentResult` / `DeploymentStatus` | Deployment result types. |
| `FeatureFlag` / `ItemType` | Enums of feature flags and supported item types. |

## FabricWorkspace Parameters

| Parameter | Required | Description |
|---|---|---|
| `workspace_id` | One of `workspace_id` / `workspace_name` | Target workspace GUID. |
| `workspace_name` | One of `workspace_id` / `workspace_name` | Target workspace display name (resolved to ID via API). |
| `repository_directory` | Yes | Local path to the directory containing Fabric items. |
| `token_credential` | Yes | Azure `TokenCredential` for API authentication. |
| `item_type_in_scope` | No | List of item type strings to deploy. Defaults to all supported types. |
| `environment` | No | Environment key for parameterization (must match `parameter.yml`). |

## publish_all_items Optional Parameters

Most require feature flags:

| Parameter | Feature Flag Required | Description |
|---|---|---|
| `item_name_exclude_regex` | None | Regex to exclude items by name. |
| `folder_path_exclude_regex` | `enable_experimental_features` + `enable_exclude_folder` | Regex to exclude folders. |
| `folder_path_to_include` | `enable_experimental_features` + `enable_include_folder` | List of folder paths to include. |
| `items_to_include` | `enable_experimental_features` + `enable_items_to_include` | List of `"item_name.item_type"` strings. |
| `shortcut_exclude_regex` | `enable_experimental_features` + `enable_shortcut_exclude` + `enable_shortcut_publish` | Regex to exclude Lakehouse shortcuts. |

Note: `folder_path_exclude_regex` and `folder_path_to_include` are mutually exclusive.

## Supported Item Types (ItemType enum)

ApacheAirflowJob, CopyJob, DataAgent, DataBuildToolJob, DataPipeline, Dataflow,
Environment, Eventhouse, Eventstream, GraphQLApi, KQLDashboard, KQLDatabase, KQLQueryset,
Lakehouse, Map, MirroredDatabase, MLExperiment, MountedDataFactory, Notebook, Ontology,
PaginatedReport, Reflex, Report, SemanticModel, SparkJobDefinition, SQLDatabase,
UserDataFunction, VariableLibrary, Warehouse

If a user names an item type not in this list, it is unsupported — do not claim it works.

## Feature Flags (FeatureFlag enum)

| Flag | Description |
|---|---|
| `enable_lakehouse_unpublish` | Enable deletion of Lakehouses. |
| `enable_warehouse_unpublish` | Enable deletion of Warehouses. |
| `enable_sqldatabase_unpublish` | Enable deletion of SQL Databases. |
| `enable_eventhouse_unpublish` | Enable deletion of Eventhouses. |
| `enable_kqldatabase_unpublish` | Enable deletion of KQL Databases (attached to Eventhouses). |
| `enable_shortcut_publish` | Enable deploying shortcuts with the Lakehouse. |
| `disable_workspace_folder_publish` | Disable deploying workspace sub folders. |
| `continue_on_shortcut_failure` | Continue deployment when shortcuts fail to publish. |
| `enable_environment_variable_replacement` | Enable use of pipeline variables. |
| `enable_experimental_features` | Gate for all experimental features. |
| `enable_items_to_include` | Enable selective publish/unpublish of items. |
| `enable_exclude_folder` | Enable folder-based exclusion during publish. |
| `enable_include_folder` | Enable folder-based inclusion during publish. |
| `enable_shortcut_exclude` | Enable selective publishing of shortcuts in a Lakehouse. |
| `enable_response_collection` | Enable collection of API responses during publish. |
| `enable_hard_delete` | Hard delete items, bypassing the workspace recycle bin. |
| `enable_bulk_publish` | Publish items using the bulk import API. |

## Environment Variables (EnvVar enum)

| Variable | Description |
|---|---|
| `FABRIC_CICD_HTTP_TRACE_ENABLED` | Enable HTTP request/response tracing (`1`/`true`/`yes`). |
| `FABRIC_CICD_HTTP_TRACE_FILE` | Path to save HTTP trace output. |
| `DEFAULT_API_ROOT_URL` | Override Power BI API root URL (default `https://api.powerbi.com`). |
| `FABRIC_API_ROOT_URL` | Override Fabric API root URL (default `https://api.fabric.microsoft.com`). |
| `FABRIC_CICD_RETRY_DELAY_OVERRIDE_SECONDS` | Override retry delay in seconds. |
| `FABRIC_CICD_RETRY_AFTER_SECONDS` | Override retry-after delay for name conflicts (default 300). |
| `FABRIC_CICD_RETRY_BASE_DELAY_SECONDS` | Override base delay for name conflict retries (default 30). |
| `FABRIC_CICD_RETRY_MAX_DURATION_SECONDS` | Override max duration for retries (default 300). |
| `FABRIC_CICD_PARALLEL_MAX_WORKERS` | Override max parallel workers (default 8). |
| `FABRIC_CICD_FILE_LOGGING_ENABLED` | Enable file logging (`1`/`true`/`yes`). Defaults to disabled. |

## Exception Types

`ParsingError`, `InputError`, `TokenError`, `InvokeError`, `ItemDependencyError`,
`FileTypeError`, `ParameterFileError`, `FailedPublishedItemStatusError`, `PublishError`
(all subclass an internal `BaseCustomError`). `ConfigValidationError` is raised for
`config.yml` schema problems.

## Authentication Methods

Authentication requires an explicit `token_credential` parameter (any Azure `TokenCredential`):

1. **Azure CLI**: `AzureCliCredential()` — local development (requires `az login` first).
2. **Azure PowerShell**: `AzurePowerShellCredential()` — local development.
3. **Service principal (secret)**: `ClientSecretCredential(tenant_id, client_id, client_secret)` — CI/CD.
4. **Service principal (certificate)**: `CertificateCredential(tenant_id, client_id, certificate_path=...)` — CI/CD.
5. **Managed identity**: `ManagedIdentityCredential()` — Azure-hosted pipelines.
6. **Workload identity federation (OIDC)**: `WorkloadIdentityCredential(tenant_id, client_id)` — secretless; recommended for GitHub Actions and Azure DevOps.
7. **Fabric notebook**: Custom `TokenCredential` wrapping `notebookutils.credentials.getToken("pbi")`.

Common auth error: `CredentialUnavailableError` — user not logged in or credential misconfigured.

## Parameterization (parameter.yml)

Four replacement types: `find_replace`, `key_value_replace` (JSONPath-based), `spark_pool`,
`semantic_model_binding`. The `parameter.yml` file must be in the root of
`repository_directory`. Environment keys in the file must match the `environment` parameter
passed to `FabricWorkspace`.

## Repository Directory Structure

```
repository_directory/
├── ItemName.ItemType/
│   ├── .platform          (required metadata)
│   └── <definition files>
├── FolderName/            (optional workspace folders)
│   └── ItemName.ItemType/
│       ├── .platform
│       └── <definition files>
└── parameter.yml          (optional parameterization)
```

## Standards & Best Practices (use only when relevant)

- **Python packaging**: PEP 440, PEP 508, PEP 517/518 — install/version/dependency issues.
- **HTTP/REST**: RFC 7231, RFC 7807, Microsoft REST API Guidelines — API errors/status codes.
- **Auth**: OAuth 2.0 (RFC 6749), OpenID Connect, MSAL — auth flows and credentials.
- **YAML**: YAML 1.2 — `parameter.yml`/`config.yml` syntax.
- **CI/CD**: Azure DevOps / GitHub Actions conventions — pipeline integration.
- **File I/O**: POSIX paths, PEP 428 (pathlib) — path handling and cross-platform behavior.

When citing a standard, mention it briefly (e.g., "per RFC 7231, a 404 indicates…") — do not
explain the standard itself.
