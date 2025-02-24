# Feature Flags

For scenarios that aren't supported by default, fabric-cicd offers `feature-flags`.

| Flag Name                  | Description                                         |
| -------------------------- | --------------------------------------------------- |
| enable_lakehouse_unpublish | Set to enable the deletion of Lakehouses            |
| disable_print_identity     | Set to disable printing the executing identity name |

<span class="md-h3-nonanchor">Example</span>

```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items, append_feature_flag
append_feature_flag("enable_lakehouse_unpublish")
workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    repository_directory="/path/to/repo",
    item_type_in_scope=["Lakehouse"]
)
publish_all_items(workspace)
unpublish_orphaned_items(workspace)
```
