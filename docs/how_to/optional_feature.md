# Optional Features

fabric-cicd has an expected default flow; however, there will always be cases where overriding default behavior is required.

## Feature Flags

For scenarios that aren't supported by default, fabric-cicd offers feature flags. Below is an exhaustive list of currently supported features.

| Flag Name                                 | Description                                                             | Experimental |
| ----------------------------------------- | ----------------------------------------------------------------------- | ------------ |
| `enable_lakehouse_unpublish`              | Set to enable the deletion of Lakehouses                                |              |
| `enable_warehouse_unpublish`              | Set to enable the deletion of Warehouses                                |              |
| `enable_sqldatabase_unpublish`            | Set to enable the deletion of SQL Databases                             |              |
| `enable_eventhouse_unpublish`             | Set to enable the deletion of Eventhouses                               |              |
| `enable_kqldatabase_unpublish`            | Set to enable the deletion of KQL Databases (attached to Eventhouses)   |              |
| `disable_print_identity`                  | Set to disable printing the executing identity name                     |              |
| `enable_shortcut_publish`                 | Set to enable deploying shortcuts with the Lakehouse                    |              |
| `enable_environment_variable_replacement` | Set to enable the use of pipeline variables                             |              |
| `disable_workspace_folder_publish`        | Set to disable deploying workspace sub folders                          |              |
| `enable_experimental_features`            | Set to enable experimental features, such as selective deployments      |              |
| `enable_items_to_include`                 | Set to enable selective publishing/unpublishing of items                | ☑️           |
| `enable_exclude_folder`                   | Set to enable folder-based exclusion during publish operations          | ☑️           |
| `enable_include_folder`                   | Set to enable folder-based inclusion during publish operations          | ☑️           |
| `enable_shortcut_exclude`                 | Set to enable selective publishing of shortcuts in a Lakehouse          | ☑️           |
| `enable_response_collection`              | Set to enable collection of API responses during publish operations     |              |
| `continue_on_shortcut_failure`            | Set to allow deployment to continue even when shortcuts fail to publish |              |

<span class="md-h3-nonanchor">Example</span>

```python
from fabric_cicd import append_feature_flag
append_feature_flag("enable_lakehouse_unpublish")
append_feature_flag("enable_warehouse_unpublish")
append_feature_flag("disable_print_identity")
append_feature_flag("enable_environment_variable_replacement")
append_feature_flag("enable_response_collection")
```

## Selective Deployment Features

By default, fabric-cicd performs a full deployment of all repository items. Selective deployment is an experimental feature due to the risk of deploying Fabric items that have dependencies on other items, which can result in broken deployments. These features support a range of filtering options, from broader folder-based selection to more granular item-level and shortcut-level filtering. To use these features, you must enable both the `enable_experimental_features` flag and the specific feature flag (if applicable).

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

**Note:** `folder_path_exclude_regex` and `folder_path_to_include` are mutually exclusive and cannot be used together for the same deployment. These filters are ignored when the `disable_workspace_folder_publish` feature flag is set. Folder-based filtering does not impact standalone items.

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

Filters are evaluated in the following order (exclusion filters are always applied before inclusion filters):

1. **`item_name_exclude_regex`** — Items matching the regex are excluded
2. **`folder_path_exclude_regex`** — Items in matching folders are excluded
3. **`items_to_include`** — Only explicitly listed items are published
4. **`folder_path_to_include`** — Only items in specified folders are published

**Note:** `folder_path_exclude_regex` and `folder_path_to_include` are mutually exclusive — only one can be used per deployment. Standalone items (items not in any folder) are not impacted by folder-level filters.

### Lakehouse Shortcut Filtering

Shortcuts are items associated with Lakehouse items and can be selectively published using the following experimental feature:

1. **`shortcut_exclude_regex`**
    - Optional parameter in `publish_all_items()`, set to a regex pattern that matches the shortcut name(s) found within Lakehouse item(s) in the repository.
    - Requires the `enable_shortcut_exclude` feature flag.
    - The matching shortcut(s) will be excluded from publishing.

**Note:** This feature can be applied along with the other selective deployment features — please be cautious when using to avoid unexpected results.

## Debugging

If an error arises, or you want full transparency to all calls being made outside the library, enable debugging. Enabling debugging will write all API calls to the terminal. The logs can also be found in the `fabric_cicd.error.log` file.

```python
from fabric_cicd import change_log_level
change_log_level("DEBUG")
```

**Note:** The `"DEBUG"` parameter can be omitted as it is the default value.

For comprehensive debugging information, including how to use the error log file and debug scripts, see the [Troubleshooting Guide](troubleshooting.md).
