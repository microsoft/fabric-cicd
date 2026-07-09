### Library Version

1.0.9

### Python Version

3.11.4

### What is the problem?

When I call `publish_all_items()` my `DataPipeline` fails with a `PublishError` because a
`Notebook` it depends on has not been published yet. I set
`item_type_in_scope=["DataPipeline"]`.

### Steps to reproduce

1. Initialize `FabricWorkspace`
2. Call `publish_all_items(workspace)`

### Actual behavior

PublishError: dependency 'SalesNotebook.Notebook' has not been published.
