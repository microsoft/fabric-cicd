# Optional Features

fabric-cicd has an expected default flow; however, there will always be cases where overriding default behavior is required.

## Feature Flags

For scenarios that aren't supported by default, fabric-cicd offers `feature-flags`. Below is an exhaustive list of currently supported features.

| Flag Name                                 | Description                                          |
| ----------------------------------------- | ---------------------------------------------------- |
| `enable_lakehouse_unpublish`              | Set to enable the deletion of Lakehouses             |
| `disable_print_identity`                  | Set to disable printing the executing identity name  |
| `enable_shortcut_publish`                 | Set to enable deploying shortcuts with the lakehouse |
| `enable_environment_variable_replacement` | Set to enable the use of pipeline variables          |
| `disable_workspace_folder_publish`        | Set to disable deploying workspace sub folders       |
| `experimental`                            | Set to enable selective deployment\*                 |

\*`experimental` flag must be used with `publish_all_items(workspace, publish_items_to_include_list)`

<span class="md-h3-nonanchor">Example</span>

```python
from fabric_cicd import append_feature_flag
append_feature_flag("enable_lakehouse_unpublish")
append_feature_flag("disable_print_identity")
append_feature_flag("enable_environment_variable_replacement")
```

## Debugging

If an error arises, or you want to have full transparency to all calls being made outside the library, enable debugging. Enabling debugging will write all API calls to the terminal and to the `fabric-cicd.log`.

```python
from fabric_cicd import change_log_level
change_log_level("DEBUG")
```
