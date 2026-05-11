# fabriccicd_inputs

Typed, per-environment input files for Fabric CI/CD. This package replaces the
old `fabriccicd.config.yml` + `lakehouse_access.xml` pair with one Python module
per workspace, plus shared schemas and constants.

## Layout

```
fabriccicd_inputs/
├── __init__.py       Assembles MSIT_TOPOLOGY + REALM_TOPOLOGY,
│                     exposes get_topology() / get_workspace()
├── _schema.py        Enums + dataclasses only (no values)
├── _common.py        Values reused across envs:
│                     PROJECT, REPO, REPO_PATH, WORKSPACE_PREFIX,
│                     MSIT_CAPACITY, REALM_CAPACITY, SECURITY_GROUP,
│                     REALM_ID, REALM_API_BASE
├── msit_dev.py       MSIT DEV workspace (+ lakehouses + access)
├── msit_test.py      MSIT TEST workspace
├── msit_prod.py      MSIT PROD workspace
├── realm_dev.py      Realm DEV workspace
├── realm_test.py     Realm TEST workspace
└── realm_prod.py     Realm PROD workspace
```

Files prefixed with `_` are internal (schemas and shared constants). The six
`<mode>_<env>.py` files are what individual env owners edit day-to-day.

## Usage

```python
from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items
from fabriccicd_inputs import get_topology, get_workspace

# Pick a topology
topology = get_topology(realm_mode=False)   # or True for realm

# Get one workspace
ws = get_workspace("DEV")                   # MSIT DEV
ws = get_workspace("DEV", realm_mode=True)  # realm DEV

# Deploy
workspace = FabricWorkspace(
    workspace_id=ws.workspace_id,
    environment=ws.target.value,
    repository_directory=topology.repo_path,
    item_type_in_scope=ws.publish.item_types_in_scope,
    token_credential=AzureCliCredential(),
)
publish_all_items(workspace)
```

## Common edits

| Task                                 | File to edit                                      |
| ------------------------------------ | ------------------------------------------------- |
| Add/change a lakehouse on MSIT DEV   | `msit_dev.py` → `LAKEHOUSES` list                 |
| Grant a user access to a lakehouse   | `msit_dev.py` → that lakehouse's `access_list`    |
| Change a workspace ID                | `<mode>_<env>.py` → `WORKSPACE.workspace_id`      |
| Change capacity / security group     | `_shared.py`                                      |
| Change repo path or workspace prefix | `_shared.py`                                      |
| Add a new field to all envs          | `_schema.py` (then update each `<mode>_<env>.py`) |

## Verifying a change

Run the built-in summary to print every topology, workspace, capacity,
access entry, and lakehouse:

```pwsh
python -c "from fabriccicd_inputs import _print_summary; _print_summary()"
```

If a typo or missing field breaks the topology, this command fails at import
time (before any Fabric API call is made).

## Adding a new environment

1. Create `fabriccicd_inputs/<mode>_<newenv>.py` modeled on an existing one.
2. Define a `WORKSPACE = WorkspaceEnvironment(...)`.
3. Register it in `__init__.py` under the appropriate topology's `workspaces`
   dict.
4. Re-run the summary command to verify.
