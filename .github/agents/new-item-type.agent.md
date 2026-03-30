---
name: New Item Type
description: Guide and assist with onboarding a new Microsoft Fabric item type into fabric-cicd
argument-hint: Tell me which Fabric item type you want to add (e.g., "Add support for Ontology")
tools:
    - runInTerminal
    - terminalLastCommand
    - search
    - fetch
    - readFile
    - editFiles
    - createFile
---

# New Item Type Onboarding Agent

> **Important:** If you are unsure about any detail — such as whether the item type supports definitions, has unique deployment requirements, depends on other item types, or requires parameterization — **always ask the requestor for clarification before proceeding**. List the specific unknowns and ask the requestor to confirm each one before continuing.

## Prerequisites

Before starting, you need only the **display name** (PascalCase, as used by the Fabric API — e.g., `CopyJob`, `SnowflakeDatabase`). The eligibility gates below will determine the remaining core details.

### Eligibility Gates

Before proceeding, confirm all of the following. If any gate fails, **stop** — the item type cannot be onboarded.

1. The item type must be supported in source control / Git integration. Fetch the [item definition overview](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/item-definition-overview) page and check if the item type appears in the **"Definition Details for Supported Item Types"** list.
2. The Fabric API must support deployment for the item type — either full definition deployment or shell-only creation (like Lakehouse/Warehouse). Verify using the steps in **"How to verify gates 2 and 3"** below.
3. The Fabric API must support service principal (SPN) authentication for the item type's deployment operations. fabric-cicd is primarily used in CI/CD pipelines where SPN is the standard authentication method. Verify using the steps in **"How to verify gates 2 and 3"** below.

#### How to verify gates 2 and 3

The Fabric REST API docs follow a predictable URL pattern. Use these steps to directly navigate to the right pages instead of searching the generic API root:

1. **Construct the item type's API items page URL:**
   `https://learn.microsoft.com/en-us/rest/api/fabric/{itemtype-lowercase}/items`
   where `{itemtype-lowercase}` is the PascalCase item type name lowercased with no separators.
    - Examples: `SnowflakeDatabase` → `snowflakedatabase`, `DataPipeline` → `datapipeline`, `KQLDatabase` → `kqldatabase`

2. **Fetch that page and confirm deployment operations exist (Gate 2):**
    - Full definition support: Look for **"Get [Item] Definition"** and **"Update [Item] Definition"** operations in the Operations table.
    - Shell-only support: If the documentation explicitly states that item definition is not supported (e.g., Warehouse's Create page states "This API does not support item definition") and the item has **"Create [Item]"** and **"Update [Item]"** operations, the item is shell-only. **Important:** There can be nuances with definition support — always present your findings to the requestor and ask them to confirm whether the item should be categorized as full definition or shell-only before proceeding.
    - If neither Create nor definition operations exist, Gate 2 fails.

3. **Fetch the Create endpoint page to check SPN support (Gate 3):**
   `https://learn.microsoft.com/en-us/rest/api/fabric/{itemtype-lowercase}/items/create-{item-type-kebab-case}`
   where `{item-type-kebab-case}` inserts hyphens between PascalCase words and lowercases everything.
    - Examples: `SnowflakeDatabase` → `create-snowflake-database`, `DataPipeline` → `create-data-pipeline`, `KQLDatabase` → `create-kql-database`
    - Find the **"Microsoft Entra supported identities"** section and confirm the table shows **"Service principal and Managed identities: Yes"**. If it shows "No" or the section is missing, Gate 3 fails.

**Important:** You MUST fetch the actual API pages listed above. Do not guess or assume gate results based on the item type name alone. If a URL returns a 404, try alternate kebab-case patterns (e.g., `graphqlapi` instead of `graph-q-l-api`), or fall back to fetching the [Fabric REST API root](https://learn.microsoft.com/en-us/rest/api/fabric/) and navigating to the item type's section. If the documentation is still inaccessible, ask the requestor to provide the relevant API page details.

**Exceptions:** Gates 1 and 3 may be excepted on a case-by-case basis with fabric-cicd team approval — for example, Notebook `.ipynb` format is supported despite not being source-controlled (gate 1 exception). Gate 2 (API deployment support) has no exceptions. Any approved exception must be documented as a known limitation in `docs/how_to/item_types.md`.

### Additional Details (Required)

After the eligibility gates pass, you **must** ask the requestor about **every row** in this table and record the answer before starting implementation. Do not skip any row or assume the answer is "no."

| Question to ask the requestor                                                                                                             | Example                                                                                                                                          | Affects     |
| ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| Which existing item types must deploy **before** this one? Which existing types depend on this one deploying first?                       | Eventhouse → KQLDatabase, SemanticModel → Report                                                                                                 | Step 1b     |
| Should certain file paths within the item folder be skipped during publish?                                                               | `.pbi/`, `.children/`                                                                                                                            | Step 1d     |
| Does the API use a non-standard definition format? If yes, provide the exact format string and instructions for how it should be handled. | `ipynb`, `SparkJobDefinitionV2`                                                                                                                  | Step 1e     |
| Does deleting this item destroy user data?                                                                                                | Lakehouse, Eventhouse                                                                                                                            | Step 1f     |
| Can items of this type reference each other?                                                                                              | Data Pipeline invokes another Data Pipeline                                                                                                      | Steps 1g, 2 |
| Does the item need custom deployment logic, dependency resolution, or special parameterization? If yes, describe what is needed.          | Lakehouse creation payload, Environment async check, KQL items need query service URI (via dynamic replacement), SemanticModel binding parameter | Step 2      |

---

## Safety Rules

- **Never hardcode secrets, tokens, or credentials** in publisher code or tests
- **Use deterministic test data** — no real tenant IDs, workspace IDs, or user emails
- **Follow existing patterns** — consistency is more important than cleverness
- **Validate all assumptions** — if unsure about API behavior, ask the requestor

---

## Integration Checklist

Once eligibility gates pass and additional details are gathered, proceed through these steps in order. If you encounter something that doesn't fit the steps below, **stop and ask the requestor** rather than improvising.

### Step 1 — Register the Item Type in Constants

**File:** `src/fabric_cicd/constants.py`

Read `constants.py` to see the existing patterns for each mapping below. Add entries following the same format.

#### 1a. Add to the `ItemType` enum

Add a new member in alphabetical order within the enum:

```python
class ItemType(str, Enum):
    # ... existing members ...
    NEW_TYPE = "NewType"
```

**Rules:**

- Enum member name uses `UPPER_SNAKE_CASE`
- Enum value uses `PascalCase` matching the Fabric API `type` field exactly

#### 1b. Add to `SERIAL_ITEM_PUBLISH_ORDER`

Choose the correct position based on the item's dependencies. Items that other items depend on must come **earlier** in the order. The unpublish order is automatically the reverse.

##### How to determine the correct position

> **How items reference each other:** In Fabric definition files, items reference other items via either **logical IDs** (workspace-agnostic GUIDs assigned by Git integration — automatically replaced by fabric-cicd) or **item IDs** (environment-specific GUIDs that differ per workspace — must be resolved via parameterization in `parameter.yml`). When reviewing definition files to identify dependencies, look for both types of references.

1. **Read the current `SERIAL_ITEM_PUBLISH_ORDER`** in `constants.py` to see the latest order.
2. **Identify upstream dependencies** — types the new item depends on. Check definition files for references (logical IDs, item IDs, paths, connection strings) and ask the requestor. The new item must go **after** the highest-numbered upstream dependency.
3. **Identify downstream dependents** — existing types that will depend on the new item. The new item must go **before** the lowest-numbered downstream dependent.
4. Place the new item anywhere in the valid range between those two bounds. If there is no gap, renumber existing entries to make room.
5. If the new item has **no dependencies in either direction**, place it at the end of the order.
6. **When in doubt, ask the requestor** — do not guess dependency relationships.

#### 1c. Optionally add to `SHELL_ONLY_PUBLISH`

If the API does **not** support item definition and only supports metadata (shell) deployment — like Warehouse, ML Experiment, etc.

#### 1d. Optionally add to `EXCLUDE_PATH_REGEX_MAPPING`

If certain file paths within the item should be excluded during publish (e.g., `.pbi/` folders for Report/SemanticModel, `.children/` for Eventhouse).

#### 1e. Optionally add to `API_FORMAT_MAPPING`

If the Fabric API requires a specific format string for the item's definition (e.g., `"ipynb"` for Notebooks, `"SparkJobDefinitionV2"` for Spark Job Definitions).

Only add an API format if the format is supported in Fabric's Git integration (source control). If the format is not source-controlled, it generally should not be added unless approved by the fabric-cicd team.

**Known exception:** Notebook `.ipynb` format is supported despite not being source-controlled. This is documented as a known limitation in `docs/how_to/item_types.md`.

#### 1f. Optionally add to `UNPUBLISH_FLAG_MAPPING`

If unpublishing the item is destructive and should be gated behind a feature flag (like Lakehouse, Warehouse, Eventhouse). If so, also add a new `FeatureFlag` enum member.

#### 1g. Optionally add to `ITEM_TYPE_TO_FILE`

If items of this type can reference other items of the **same** type (intra-type dependencies), register the content file that contains those references so the dependency module knows which file to parse. This is required when implementing intra-type dependency ordering (see Step 2 — Dependency Ordering below).

---

### Step 2 — Create a Publisher Class

**File:** `src/fabric_cicd/_items/_newtype.py` (new file)

Create a publisher class that extends `ItemPublisher`. The simplest case:

```python
# src/fabric_cicd/_items/_newtype.py
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions to process and deploy NewType item."""

from fabric_cicd._items._base_publisher import ItemPublisher
from fabric_cicd.constants import ItemType


class NewTypePublisher(ItemPublisher):
    """Publisher for NewType items."""

    item_type = ItemType.NEW_TYPE.value
```

For more complex items, you can override these methods from `ItemPublisher`:

| Method                          | Purpose                                 | When to Override                                         |
| ------------------------------- | --------------------------------------- | -------------------------------------------------------- |
| `publish_one(item_name, _item)` | Custom publish logic per item           | Custom file processing, exclude paths, creation payloads |
| `get_items_to_publish()`        | Filter or order items before publishing | Custom item filtering                                    |
| `get_unpublish_order(items)`    | Dependency-aware unpublish ordering     | **Must also set `has_dependency_tracking = True`**       |
| `pre_publish_all()`             | Pre-publish checks                      | e.g., Environment publish state check                    |
| `post_publish_all()`            | Post-publish actions                    | e.g., Semantic Model connection binding                  |
| `post_publish_all_check()`      | Async publish state verification        | **Must also set `has_async_publish_check = True`**       |

#### Inter-Type Dependency Handling

If the new item's definition contains references to other item types that need resolving at deploy time, implement one of these patterns:

| Pattern                          | When to use                                                                  | Example                               | Key mechanism                                                                                                                          |
| -------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| **Property attribute lookup**    | Needs a runtime attribute (SQL endpoint, query URI) from an upstream item    | `_kqlqueryset.py`, `_kqldashboard.py` | `pre_publish_all()` → `_refresh_deployed_items()`, then look up `workspace_obj.deployed_items[upstream_type]` in `func_process_file()` |
| **Path-to-ID resolution**        | Definition contains a relative path to another item type                     | `_report.py`                          | `workspace_obj._convert_path_to_id(upstream_type, path)` in `func_process_file()`                                                      |
| **ID-to-name resolution**        | Definition contains GUIDs referencing another item type                      | `_datapipeline.py`                    | `workspace_obj._convert_id_to_name(item_type, guid, lookup_type)`                                                                      |
| **Parameterization (automatic)** | Reference handled by `find_replace` / `key_value_replace` in `parameter.yml` | Most simple items                     | No publisher code needed                                                                                                               |

If the new item is an **upstream dependency** (other existing items will reference it), check whether you need to add it to `PROPERTY_PATH_ATTR_MAPPING` in `constants.py` and whether existing downstream publishers need updates. Ask the requestor.

#### Intra-Type Dependency Ordering (DAG)

If items of the same type can reference each other (e.g., a pipeline invoking another pipeline, a dataflow sourcing from another dataflow), publish and unpublish order must respect those internal dependencies. This requires:

1. **A reference-finding function** that scans an item's content file and returns names of other items (of the same type) it depends on.
2. **Sequential publish with dependency ordering** — set `has_dependency_tracking = True` and configure `parallel_config = ParallelConfig(enabled=False, ordered_items_func=...)` with a function that returns item names in topological order.
3. **Dependency-aware unpublish** — override `get_unpublish_order()` to return items in reverse dependency order.
4. **Choose or implement a sorting strategy:**
    - **Reuse `_manage_dependencies.py`** (preferred) — provides generic topological sort via `set_publish_order()` and `set_unpublish_order()`. You supply a `find_referenced_items_func(workspace, content, lookup_type) -> list[str]` callback. Used by `DataPipeline`. Requires `ITEM_TYPE_TO_FILE` registration (Step 1g).
    - **Custom DFS** — if the dependency resolution has unique requirements (e.g., Dataflow's parameterization-aware source detection), implement a custom ordering function as done in `_dataflowgen2.py`.

See `_datapipeline.py` (generic topological sort) and `_dataflowgen2.py` (custom DFS) for reference implementations.

#### Custom File Processing Callback (`func_process_file`)

The standard publish pipeline automatically handles logical ID replacement, parameterization, and workspace ID replacement for all item types. If the item type requires **additional, item-type-specific content transformations** that the generic pipeline cannot handle, define a module-level `func_process_file(workspace_obj, item_obj, file_obj) -> str` callback and pass it to `_publish_item()` in `publish_one()`. This callback runs **first**, before the generic pipeline steps.

See `_report.py`, `_kqldashboard.py`, or `_kqlqueryset.py` for examples of this pattern.

#### Parameterization

Generic parameterization (`find_replace`, `key_value_replace`) is applied automatically to all item types — no publisher code needed. If the item type requires a **specialized parameter key** in `parameter.yml` (e.g., Environment's `spark_pool`, SemanticModel's `semantic_model_binding`):

- If the specialized parameterization is **not required for deployment** (e.g., connection binding can be done later), proceed with onboarding and coordinate with the fabric-cicd team to add it separately.
- If the specialized parameterization **blocks deployment** (e.g., the item cannot be deployed without it), coordinate with the fabric-cicd team before completing the integration — the item type should not be onboarded until the parameterization is supported.

---

### Step 3 — Register the Publisher in the Factory Method

**File:** `src/fabric_cicd/_items/_base_publisher.py`

Update the `ItemPublisher.create()` factory method — add an import for the new publisher class and a mapping entry in `publisher_mapping`.

**Rules:**

- Follow the same ordering as `SERIAL_ITEM_PUBLISH_ORDER` for the mapping dictionary
- Import must be inside the `create()` method (lazy imports to avoid circular dependencies)

---

### Step 4 — Add Tests

**Directory:** `tests/`

Create or update test files for the new item type:

- **Unit tests:** Add tests in `tests/test_publish.py` using the `create_test_item()` helper to create item data inline. For publisher-specific behavior, see `tests/test_environment_publish.py` as an example.
- **Integration tests:** Add the item type to `sample/workspace/` and to the `item_types_to_deploy` list in `tests/test_integration_publish.py`. The HTTP trace (`tests/fixtures/http_trace.json.gz`) must be regenerated against a real Fabric workspace — see `tests/fixtures/README.md`. This step requires human intervention.

**Rules:**

- Use deterministic test data — no real tenant IDs, workspace IDs, or user emails
- Never hardcode secrets, tokens, or credentials in tests
- Mock all API interactions using the existing test patterns

---

### Step 5 — Documentation Updates

#### 5a. Supported Item Types List (auto-generated)

The supported item types list auto-generates from the `ItemType` enum via `docs/config/pre-build/update_item_types.py` — **no manual update is needed for the list**.

#### 5b. Item Types How-To Page

**File:** `docs/how_to/item_types.md`

Add a new section for the item type following the existing pattern.

---

### Step 6 — Sample Files and Deployment Validation

**Do not create sample files yourself.** Sample items must come from a real Fabric workspace exported via Git integration — the agent cannot generate valid definition files.

Encourage the requestor to:

1. **Add a sample item** to `sample/workspace/` by exporting an instance of the item type from a Fabric workspace connected to Git. The exported folder should follow the naming convention `{DisplayName}.{ItemType}/`.
2. **Add the item type** to `item_type_in_scope` in `devtools/debug_local.py` and run a sample deployment against a real workspace to validate end-to-end behavior.
3. **Add the item type** to `item_types_to_deploy` in `tests/test_integration_publish.py`. The HTTP trace (`tests/fixtures/http_trace.json.gz`) must also be regenerated against a real workspace — this is a manual step.

---

## Patterns and Reference Examples

**Default workflow**: All item types require steps 1a, 1b, 2, 3, 4, 5, 6. Use this table to determine which optional steps (1c, 1d, 1e, 1f, 1g) to skip based on your item type's characteristics. Read the example files for implementation details.

| Pattern                          | Skip Steps         | Example Files                                       | Key Details                                                                                    |
| -------------------------------- | ------------------ | --------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Simple** (no special behavior) | 1c, 1d, 1e, 1f, 1g | `_graphqlapi.py`, `_copyjob.py`                     | Default publish behavior, no overrides needed                                                  |
| **Exclude paths**                | 1c, 1e, 1f, 1g     | `_dataagent.py`, `_eventhouse.py`                   | Override `publish_one()` to pass `exclude_path` — do step 1d                                   |
| **API format**                   | 1c, 1d, 1f, 1g     | `_notebook.py`                                      | Override `publish_one()` to pass `api_format` — do step 1e                                     |
| **Custom file processing**       | 1c, 1d, 1e, 1f, 1g | `_report.py`, `_kqldashboard.py`, `_kqlqueryset.py` | Define `func_process_file` callback, pass to `_publish_item()`                                 |
| **Shell-only** (metadata only)   | 1d, 1e, 1f, 1g     | `_warehouse.py`, `_lakehouse.py`                    | May need creation payload logic in `publish_one()` — do step 1c                                |
| **Destructive unpublish**        | 1c, 1d, 1e, 1g     | `_lakehouse.py`, `_eventhouse.py`                   | Add new `FeatureFlag` enum member — do step 1f                                                 |
| **Intra-type dependencies**      | 1c, 1d, 1e, 1f     | `_datapipeline.py`, `_dataflowgen2.py`              | Set `has_dependency_tracking`, `ParallelConfig`, override `get_unpublish_order()` — do step 1g |
| **Post-publish actions**         | 1c, 1d, 1e, 1f, 1g | `_semanticmodel.py`                                 | Override `post_publish_all()`                                                                  |
| **Async publish check**          | 1c, 1d, 1e, 1f, 1g | `_environment.py`                                   | Set `has_async_publish_check = True`, override `post_publish_all_check()`                      |

**Combined patterns**: If your item type needs multiple patterns, skip only steps that appear in ALL applicable Skip Steps columns.

---

## Final Validation

After completing all steps, run the mandatory validation suite:

```
uv run python -c "from fabric_cicd import FabricWorkspace; print('Import successful')"
uv run pytest -v
uv run ruff format
uv run ruff check
```

All four must pass before the task is complete.

---

## Key Files Quick Reference

| File                                             | Purpose                                                         |
| ------------------------------------------------ | --------------------------------------------------------------- |
| `src/fabric_cicd/constants.py`                   | Item type enum, publish order, feature flags, all type mappings |
| `src/fabric_cicd/_items/_base_publisher.py`      | Base publisher class and factory method                         |
| `src/fabric_cicd/_items/`                        | All item publisher implementations                              |
| `src/fabric_cicd/_items/_manage_dependencies.py` | Generic topological sort for intra-type dependencies            |
| `src/fabric_cicd/fabric_workspace.py`            | Main workspace management class                                 |
| `src/fabric_cicd/publish.py`                     | Top-level publish/unpublish orchestration                       |
| `tests/`                                         | All test files                                                  |
| `tests/fixtures/`                                | Test fixture data                                               |
| `docs/how_to/item_types.md`                      | Per-item-type documentation                                     |
| `docs/config/pre-build/update_item_types.py`     | Auto-generates supported item types list from enum              |
| `sample/workspace/`                              | Example workspace item structures                               |
