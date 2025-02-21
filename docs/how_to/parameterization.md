# Parameterization

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field in the `FabricWorkspace` class. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped.

Raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) for additional parameterization capabilities.

## find_replace

For generic find-and-replace operations. This will replace every instance of a specified string in every file. Specify the `find` value as the key and the `replace` value for each environment. See example at the end for a complete yaml file.

Note: A common use case for this function is to replace connection strings. I.e. find and replace a connection guid referenced in data pipeline.

```yaml
find_replace:
    <find-this-value>:
        <environment-1>: <replace-with-this-value>
        <environment-2>: <replace-with-this-value>
```

## spark_pool

Environments attached to custom spark pools need to be parameterized because the `instance-pool-id` in the `Sparkcompute.yml` file isn't supported in the create/update environment APIs. Provide the `instance-pool-id` as the key, and the pool type and name as the values.

Environment parameterization(PPE/PROD) is not supported. If needed, raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml).

```yaml
spark_pool:
    <instance-pool-id>:
        type: <Capacity-or-Workspace>
        name: <pool-name>
```

## Parameter.yml File

<!--prettier-ignore-->
```yml
{% include "../../sample/workspace/parameter.yml" %}
```
