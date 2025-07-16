# Parameterization Overview

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field passed into the `FabricWorkspace` object. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped. This file should sit in the root of the `repository_directory` folder specified in the FabricWorkspace object.

Example of parameter.yml location based on provided repository directory:

```python
from fabric_cicd import FabricWorkspace
workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    repository_directory="C:/dev/workspace",
    item_type_in_scope=["Notebook"]
)
```

```
C:/dev/workspace
    /HelloWorld.Notebook
        ...
    /GoodbyeWorld.Notebook
        ...
    /parameter.yml
```

Raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) for additional parameterization capabilities.

## Inputs

### `find_replace`

For generic find-and-replace operations. This will replace every instance of a specified string in every file. Specify the `find_value` and the `replace_value` for each environment (e.g., PPE, PROD).

Note: A common use case for this function is to replace values in text based file types like notebooks.

```yaml
find_replace:
    # Value must be a string
    - find_value: <find-this-value>
      replace_value:
          <environment-1-key>: <replace-with-this-value>
          <environment-2-key>: <replace-with-this-value>
```

### `key_value_replace`

Provides the ability to perform key based replacement operations in JSON and YAML files. This will look for a specific key using a valid JSONPath expression and replace every found instance in every file. Specify the `find_value` and the `replace_value` for each environment (e.g., PPE, PROD). Refer to https://jsonpath.com/ for a simple to use JSONPath evaluator.

Note: A common use case for this function is to replace values in key/value file types like Pipelines, Platform files, etc. e.g., find and replace a connection GUID referenced in a data pipeline.

```yaml
key_value_replace:
    - find_key: <find-this-key> # Key must be JSONPath
      replace_value:
          <environment-1-key>: <replace-with-this-value>
          <environment-2-key>: <replace-with-this-value>
```

### `spark_pool`

Environments attached to custom spark pools need to be parameterized because the `instance_pool_id` in the `Sparkcompute.yml` file isn't supported in the create/update environment APIs. Provide the `instance_pool_id` value, and the pool `type` and `name` values as the `replace_value` for each environment (e.g., PPE, PROD).

```yaml
spark_pool:
    # Value must be a string
    - instance_pool_id: <instance-pool-id-value>
      replace_value:
          <environment-1-key>:
              type: <Capacity-or-Workspace>
              name: <pool-name>
          <environment-2-key>:
              type: <Capacity-or-Workspace>
              name: <pool-name>
```
