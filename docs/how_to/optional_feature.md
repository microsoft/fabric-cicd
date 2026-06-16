# Optional Features

fabric-cicd has an expected default flow; however, there will always be cases where overriding default behavior is required.

## Feature Flags

For scenarios that aren't supported by default, fabric-cicd offers feature flags. Below is an exhaustive list of currently supported features, grouped by category.

Flags marked as **Experimental** (☑️) require the `enable_experimental_features` flag to be set in addition to the specific flag:

```python
from fabric_cicd import append_feature_flag
append_feature_flag("enable_experimental_features")
append_feature_flag("<specific_flag>")
```

<span class="md-h3-nonanchor">Publish behavior</span>

| Flag Name                                 | Description                                                                                    | Experimental |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------- | ------------ |
| `enable_bulk_publish`                     | Deploy all items in a single API call instead of one at a time (uses the bulk import beta API) | ☑️           |
| `enable_shortcut_publish`                 | Deploy shortcuts with the Lakehouse                                                            |              |
| `continue_on_shortcut_failure`            | Allow deployment to continue even when shortcuts fail to publish                               |              |
| `disable_workspace_folder_publish`        | Disable deploying workspace sub folders                                                        |              |
| `enable_environment_variable_replacement` | Enable the use of pipeline variables for parameterization                                      |              |

<span class="md-h3-nonanchor">Unpublish behavior</span>

| Flag Name                      | Description                                                                                        |
| ------------------------------ | -------------------------------------------------------------------------------------------------- |
| `enable_lakehouse_unpublish`   | Enable the deletion of Lakehouses                                                                  |
| `enable_warehouse_unpublish`   | Enable the deletion of Warehouses                                                                  |
| `enable_sqldatabase_unpublish` | Enable the deletion of SQL Databases                                                               |
| `enable_eventhouse_unpublish`  | Enable the deletion of Eventhouses                                                                 |
| `enable_kqldatabase_unpublish` | Enable the deletion of KQL Databases (attached to Eventhouses)                                     |
| `enable_hard_delete`           | Enable hard deletion of items, bypassing the workspace recycle bin. Requires workspace Admin role. |

<span class="md-h3-nonanchor">Selective deployment</span>

| Flag Name                 | Description                                                | Experimental |
| ------------------------- | ---------------------------------------------------------- | ------------ |
| `enable_items_to_include` | Enable selective publishing/unpublishing of specific items | ☑️           |
| `enable_exclude_folder`   | Enable folder-based exclusion during publish operations    | ☑️           |
| `enable_include_folder`   | Enable folder-based inclusion during publish operations    | ☑️           |
| `enable_shortcut_exclude` | Enable selective publishing of shortcuts in a Lakehouse    | ☑️           |

<span class="md-h3-nonanchor">Diagnostics</span>

| Flag Name                    | Description                                                                |
| ---------------------------- | -------------------------------------------------------------------------- |
| `enable_response_collection` | Enable collection of API responses during publish and unpublish operations |

<span class="md-h3-nonanchor">Example</span>

```python
from fabric_cicd import append_feature_flag
append_feature_flag("enable_lakehouse_unpublish")
append_feature_flag("enable_warehouse_unpublish")
append_feature_flag("enable_environment_variable_replacement")
append_feature_flag("enable_response_collection")
```

## Debugging

If an error arises, or you want full transparency to all calls being made outside the library, enable debugging. Enabling debugging will write all API calls to the terminal. The logs can also be found in the `fabric_cicd.error.log` file.

```python
from fabric_cicd import change_log_level
change_log_level("DEBUG")
```

**Note:** The `"DEBUG"` parameter can be omitted as it is the default value.

For comprehensive debugging information, including how to use the error log file and debug scripts, see the [Troubleshooting Guide](troubleshooting.md).

## Bulk Publish

!!! warning "Experimental"

    Bulk publish deploys all items in a single API call instead of publishing them one at a time. It uses the Fabric [bulk import API](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/bulk-import-item-definitions(beta)), which is currently in beta. This feature applies to publishing only — unpublishing is not affected.

To enable bulk publish, set both `enable_experimental_features` and `enable_bulk_publish` feature flags:

=== "Programmatic"

    ```python
    from fabric_cicd import FabricWorkspace, publish_all_items, append_feature_flag

    append_feature_flag("enable_experimental_features")
    append_feature_flag("enable_bulk_publish")

    workspace = FabricWorkspace(
        workspace_id="your-workspace-id",
        repository_directory="/path/to/repo",
        item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
        token_credential=token_credential,
    )

    publish_all_items(workspace)
    ```

=== "Config-based"

    ```yaml
    core:
        workspace_id: "your-workspace-id"
        repository_directory: "/path/to/repo"
        item_types_in_scope:
            - Notebook
            - DataPipeline
            - Environment

    features:
        - enable_experimental_features
        - enable_bulk_publish
    ```

    ```python
    from fabric_cicd import deploy_with_config
    from azure.identity import AzureCliCredential

    deploy_with_config(
        config_file_path="/path/to/config.yml",
        token_credential=AzureCliCredential(),
    )
    ```

### How it works

With bulk publishing, there is no predefined order of item types. The API leverages a dependency graph under the hood to determine the correct publishing sequence. As long as logical IDs are used to reference Fabric items, the API handles the rest — simplifying dependency management overhead. This may also reduce the complexity of your parameter file, though parameterization remains a key feature for edge cases and environment-specific customizations.

Since this feature is experimental, it is recommended for non-production environments only. We encourage you to use this period to **provide feedback and help shape the bulk publish experience.** The initial release does not support all fabric-cicd item types and lacks support for more advanced parameterization features, specifically dynamic replacement variables (`$workspace`, `$items`).

!!! tip "Items using item IDs instead of logical IDs"

    Item definitions in the same workspace that reference other items by item ID (rather than logical ID) can still be published via bulk publish. However, parameterization is required to ensure references are correctly re-pointed in the target workspace. The recommended approach is to use a `find_replace` parameter where the `find_value` is the referenced item's ID and the `replace_value` is its logical ID. For example, a Dataflow Gen2 item that references a Lakehouse by item ID — use `find_replace` to swap the Lakehouse's item ID with its logical ID so the bulk publish API can resolve the dependency.

    Note that logical ID replacement may not be a valid solution for all item types. In such cases, you can either use standard publish where dynamic replacement variables are supported, or use a multi-phased bulk publish deployment by applying [selective deployment](#selective-deployment-features) to separate the affected items across phases.

    For more details on logical IDs, see [Resolve Logical ID Conflicts in Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/logical-id-conflict-resolution).

### Current Limitations

- **Unsupported item types**: `DataBuildToolJob` and `Warehouse` are not supported in bulk publish mode. If any unsupported item type is in scope, the deployment automatically falls back to standard publish mode. If `item_type_in_scope` is not specified, standard publish mode is always used. Note that supported item types still in preview (e.g., `Ontology`) may produce errors for certain item definitions.
- **Dynamic parameter variables**: Parameter files that use `$workspace` or `$items` dynamic replacement variables are not compatible with bulk publish. The deployment falls back to standard publishing when dynamic variables are detected.
- **Item count limit**: A maximum of 1,000 items can be published in a single bulk call. Exceeding this limit raises an error. Publish time may increase with higher item counts.

For common bulk publish errors and their solutions, see the [Troubleshooting Guide](troubleshooting.md#bulk-publish-failures).

## Selective Deployment Features

By default, fabric-cicd performs a full deployment of all repository items. Selective deployment is an experimental feature due to the risk of deploying Fabric items that have dependencies on other items, which can result in broken deployments. These features support a range of filtering options, from broader folder-based selection to more granular item-level and shortcut-level filtering. To use these features, you must enable both the `enable_experimental_features` flag and the specific feature flag (if applicable). **All selective deployment features are supported in both standard and bulk publish modes.**

**Warning:** Selective deployment is not recommended due to potential issues with dependency management.

### Folder-Level Filtering

A subset of items in the repository that exist within a Fabric workspace folder can be published using one of the following experimental features. Only one of these features can be applied during a deployment. Use case: selectively deploy a **group** of Fabric items (must be contained within folders). Folder-based item exclusion/inclusion is not supported in the unpublish scenario.

1. **`folder_path_exclude_regex`**
    - Optional parameter in `publish_all_items()`, set to a regex pattern that matches Fabric folder path(s) containing items in the repository.
    - Requires the `enable_exclude_folder` feature flag.
    - The folder path(s) and items contained within that match the regex will be excluded from the publish operation.
    - When using `folder_path_exclude_regex`, the pattern is matched using `search()` (substring match), so a pattern like `subfolder1` will match any folder path containing "subfolder1" (e.g., `/subfolder1`, `/subfolder1/subfolder2`, `/other/subfolder1`).
    - To target a specific folder, use an anchored pattern (e.g., `^/subfolder1$`) — this ensures only the exact folder path matches.
    - Child folders like `/subfolder1/subfolder2` will also be excluded automatically since their parent folder was excluded, preserving a consistent folder hierarchy.

2. **`folder_path_to_include`**
    - Optional parameter in `publish_all_items()`, set to a list of strings that exactly match the folder path(s) containing items in the repository.
    - Requires the `enable_include_folder` feature flag.
    - Folder paths must start with `/` (e.g., `/folder_name` or `/folder_name/nested_folder`). The matching folder path(s) and their contained items will be included in the publish operation; any other items contained within Fabric folders will be excluded.
    - When using `folder_path_to_include` with nested paths (e.g., `/subfolder1/subfolder2`), ancestor folders (e.g., `/subfolder1`) are automatically created to preserve the correct folder hierarchy, but items directly under the ancestor folder are **not** published unless the ancestor folder is also explicitly included in the list.

**Note:** `folder_path_exclude_regex` and `folder_path_to_include` are mutually exclusive and cannot be used together for the same deployment. These filters are ignored when the `disable_workspace_folder_publish` feature flag is set. Root-level items (items not in any folder) are not impacted by either folder-level filter.

### Item-Level Filtering

A subset of items in the repository can be published/unpublished using one of the following features. Both features are technically supported, but **it is recommended to use one feature per deployment to avoid unexpected results**.

1. **`item_name_exclude_regex`**
    - Optional parameter in `publish_all_items()` and `unpublish_all_orphan_items()`, set to a regex pattern that matches item name(s) found in the repository.
    - **This feature does not require any feature flags.**
    - Fabric items that match the regex will be excluded from the publish/unpublish operation.

2. **`items_to_include`**
    - Optional parameter in `publish_all_items()` and `unpublish_all_orphan_items()`, set to a list of strings that exactly match items in the repository.
    - Requires the `enable_items_to_include` feature flag.
    - Must be in the format: `"item_name.item_type"`. The matching item(s) will be included in the publish/unpublish operation.

**Note:** `item_name_exclude_regex` and `items_to_include` can be applied to items within Fabric folders or standalone items. Item-level filtering can be combined with folder-level filtering, but be cautious when using both to avoid unexpected results.

### Filter Precedence

Filters are evaluated in the following order:

1. **`items_to_include`** — Scope is narrowed upfront; only explicitly listed items proceed to further checks
2. **`item_name_exclude_regex`** — Items matching the regex are excluded
3. **`folder_path_exclude_regex`** — Items in matching folders are excluded
4. **`folder_path_to_include`** — Only items in specified folders are published

**Note:** `folder_path_exclude_regex` and `folder_path_to_include` are mutually exclusive — only one can be used per deployment. Root-level items (items not in any folder) are not impacted by either folder-level filter. When `items_to_include` is combined with exclusion filters, an item must first be in the include list before exclusion filters are evaluated against it.

### Lakehouse Shortcut Filtering

Shortcuts are items associated with Lakehouse items and can be selectively published using the following experimental feature:

1. **`shortcut_exclude_regex`**
    - Optional parameter in `publish_all_items()`, set to a regex pattern that matches the shortcut name(s) found within Lakehouse item(s) in the repository.
    - Requires the `enable_shortcut_exclude` feature flag.
    - The matching shortcut(s) will be excluded from publishing.

**Note:** This feature can be applied along with the other selective deployment features — please be cautious when using to avoid unexpected results.

## Git-Based Change Detection

`get_changed_items()` is a public utility function that uses `git diff` to detect which Fabric items have been added, modified, or renamed relative to a given git reference. It returns a list of strings in `"item_name.item_type"` format that can be passed directly to `items_to_include` in `publish_all_items()`.

While `get_changed_items()` itself requires no feature flags, passing its output to `items_to_include` requires the experimental feature flags.

**Important:** If `get_changed_items()` returns an empty list (no changes detected), do not call `publish_all_items()` without an explicit `items_to_include` list, as this would default to a full deployment. Always guard against the empty-list case:

```python
from fabric_cicd import FabricWorkspace, publish_all_items, get_changed_items, append_feature_flag

append_feature_flag("enable_experimental_features")
append_feature_flag("enable_items_to_include")

workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    repository_directory="/path/to/repo",
    item_type_in_scope=["Notebook", "DataPipeline"],
    token_credential=token_credential,  # or any other TokenCredential
)

changed = get_changed_items(workspace.repository_directory)

if changed:
    publish_all_items(workspace, items_to_include=changed)
else:
    print("No changed items detected — skipping deployment.")
```

To compare against a branch or a specific commit instead of the previous commit, pass a custom `git_compare_ref`:

```python
changed = get_changed_items(workspace.repository_directory, git_compare_ref="main")
```

**Note:** `get_changed_items()` returns only items that were **modified or added** (i.e., candidates for publishing). It does not return deleted items. Passing `items_to_include` to `publish_all_items()` requires enabling the `enable_experimental_features` and `enable_items_to_include` feature flags.
