# Optional Features

fabric-cicd has an expected default flow; however, there will always be cases where overriding default behavior is required.

## Feature Flags

For scenarios that aren't supported by default, fabric-cicd offers `feature-flags`. Below is an exhaustive list of currently supported features.

| Flag Name                                 | Description                                                        | Experimental |
| ----------------------------------------- | ------------------------------------------------------------------ | ------------ |
| `enable_lakehouse_unpublish`              | Set to enable the deletion of Lakehouses                           |              |
| `disable_print_identity`                  | Set to disable printing the executing identity name                |              |
| `enable_shortcut_publish`                 | Set to enable deploying shortcuts with the lakehouse               |              |
| `enable_environment_variable_replacement` | Set to enable the use of pipeline variables                        |              |
| `disable_workspace_folder_publish`        | Set to disable deploying workspace sub folders                     |              |
| `enable_experimental_features`            | Set to enable experimental features, such as selective deployments |              |
| `enable_items_to_include`                 | Set to enable selective publishing/unpublishing of items           | ☑️           |

<span class="md-h3-nonanchor">Example</span>

```python
from fabric_cicd import append_feature_flag
append_feature_flag("enable_lakehouse_unpublish")
append_feature_flag("disable_print_identity")
append_feature_flag("enable_environment_variable_replacement")
```

<span class="md-h3-nonanchor">Experimental Features</span>

To use experimental features, such as selective deployments (e.g., specifying a list of items to publish/unpublish), you must enable both the `enable_experimental_features` flag and the flag specific to the feature, such as `enable_items_to_include`.

## Debugging

If an error arises, or you want to have full transparency to all calls being made outside the library, enable debugging. Enabling debugging will write all API calls to the terminal and to the `fabric-cicd.log`.

```python
from fabric_cicd import change_log_level
change_log_level("DEBUG")
```
