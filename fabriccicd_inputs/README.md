# fabriccicd_inputs

Typed, per-environment input files for Fabric CI/CD. Each env file is fully
self-contained — no shared `_common` module. Edit the env you care about and
every value (workspace prefix, capacity, source-control, tenant, lakehouses,
SJDs, Spark environments, pipelines) is right there in front of you.

## Layout

```
fabriccicd_inputs/
├── __init__.py       Exposes MSIT_WORKSPACES, REALM_WORKSPACES, get_workspace()
├── _schema.py        Enums + dataclasses only (no values)
├── msit_dev.py       MSIT DEV workspace — fully self-contained
├── msit_test.py      MSIT TEST workspace — fully self-contained
├── msit_prod.py      MSIT PROD workspace — fully self-contained
├── realm_dev.py      Realm DEV workspace — fully self-contained
├── realm_test.py     Realm TEST workspace — fully self-contained
└── realm_prod.py     Realm PROD workspace — fully self-contained
```

Files prefixed with `_` are internal. The six `<mode>_<env>.py` files are what
env owners edit day-to-day.

## What each env file declares

Every `<mode>_<env>.py` defines a single `WORKSPACE = WorkspaceEnvironment(...)`
with all values inlined:

| Field                     | Purpose                                                                 |
| ------------------------- | ----------------------------------------------------------------------- |
| `target`                  | `TargetEnvironment.DEV` / `TEST` / `PROD`                               |
| `workspace_prefix`        | Required. Final name = `<prefix>-<env>` unless overridden.              |
| `workspace_name`          | Optional. If set, overrides `<prefix>-<env>` entirely.                  |
| `workspace_id`            | GUID. Empty on a clean slate — the driver persists it back after create.|
| `capacity`                | `FabricCapacity(capacity_id=..., label=...)`                            |
| `metadata`                | `ProjectMetadata` with `tenant_id`                                      |
| `repo_path`               | Local path to the cloned Fabric folder                                  |
| `source_control`          | Git provider, org, project, repo, branch, directory                     |
| `access_control`          | List of `Identity` (security groups / users / SPs + workspace roles)    |
| `lakehouses`              | List of `LakehouseDefinition` with `access_list`/`table_access`/`file_access` |
| `spark_environments`      | List of `SparkEnvironment` (runtime, spark properties, libraries, pool) |
| `spark_job_definitions`   | List of `SparkJobDefinition` (executable, main class, args, libs, lakehouse, env) |
| `pipelines`               | List of `Pipeline` with activity-level definition                       |
| `item_types_in_scope`     | Which Fabric item types to deploy                                       |
| `api_base`, `realm_mode`, `realm_id` | API targeting (MSIT vs dailyapi/realm)                       |

## Usage

```python
from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items
from fabriccicd_inputs import get_workspace

# Get one workspace — pulls everything from its env file.
ws = get_workspace("DEV")                   # MSIT DEV
ws = get_workspace("DEV", realm_mode=True)  # Realm DEV

# Deploy via fabric-cicd.
workspace = FabricWorkspace(
    workspace_id=ws.workspace_id,
    environment=ws.target.value,
    repository_directory=ws.repo_path,
    item_type_in_scope=ws.item_types_in_scope,
    token_credential=AzureCliCredential(),
)
publish_all_items(workspace)
```

In practice you don't call this directly — the `fabriccicd.py` driver in the
repo root wraps the full workflow (`create`, `connect`, `deploy`, `generate`,
`create-lakehouses`, `permissions`, etc.).

## Common edits

| Task                                  | File to edit                                            |
| ------------------------------------- | ------------------------------------------------------- |
| Change the workspace prefix or name   | `<mode>_<env>.py` → `workspace_prefix` / `workspace_name` |
| Change a workspace ID                 | `<mode>_<env>.py` → `workspace_id`                       |
| Add/change a lakehouse                | `<mode>_<env>.py` → `lakehouses=[...]`                   |
| Grant a user access to a lakehouse    | `<mode>_<env>.py` → that lakehouse's `access_list`       |
| Add a Spark Job Definition            | `<mode>_<env>.py` → `spark_job_definitions=[...]`        |
| Add a Spark Environment               | `<mode>_<env>.py` → `spark_environments=[...]`           |
| Add a Pipeline                        | `<mode>_<env>.py` → `pipelines=[...]`                    |
| Change capacity / security group      | `<mode>_<env>.py` → `capacity` / `access_control`        |
| Change repo path or branch            | `<mode>_<env>.py` → `repo_path` / `source_control`       |
| Add a new field to all envs           | `_schema.py` (then update each `<mode>_<env>.py`)        |

## Generated artifacts (SJD / Environment / Pipeline)

For Spark Job Definitions, Spark Environments, and Pipelines, the driver
generates the Fabric Git folder layout (`<name>.SparkJobDefinition/`,
`<name>.Environment/`, `<name>.DataPipeline/`) from your typed input before
calling `publish_all_items`.

- **SJD source files** (`Main/<executable_file>`, `Libs/<additional_library_uris>`)
  must already exist in the repo — the driver only emits metadata
  (`.platform` + `SparkJobDefinitionV1.json`).
- **Lakehouse and Environment IDs** are written as empty strings in the
  generated SJD JSON. Resolve them per-env via `parameter.yml` (fabric-cicd's
  standard mechanism).
- **logicalIds** are deterministic UUIDv5 derived from `(env, type, name)`, so
  re-generation is idempotent.

Run generation alone (without deploying):

```pwsh
python fabriccicd.py generate DEV
```

## Verifying a change

Run the built-in summary to print every workspace, capacity, access entry,
lakehouse, SJD, Spark env, and pipeline:

```pwsh
python -c "from fabriccicd_inputs import _print_summary; _print_summary()"
```

If a typo or missing required field breaks an env, this command fails at
import time (before any Fabric API call is made).

## Adding a new environment

1. Create `fabriccicd_inputs/<mode>_<newenv>.py` modeled on an existing one.
2. Define a `WORKSPACE = WorkspaceEnvironment(...)` with every required field
   (`target`, `workspace_prefix`, `capacity`, `metadata`, `repo_path`,
   `source_control`).
3. Register it in `__init__.py` under `MSIT_WORKSPACES` or `REALM_WORKSPACES`.
4. Re-run the summary command to verify.
