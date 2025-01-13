# Getting Started

Fabric CICD is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library supports code-first CICD integrations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal is to assist CICD developers who prefer not to interact directly with the Microsoft Fabric APIs.

## Base Expectations

-   Full deployment every time, without considering commit diffs
-   Deploys into the tenant of the executing identity

## Supported Item Types

The following item types are supported by the library:

-   Notebooks
-   Data Pipelines
-   Environments

## Authentication

-   You can optionally provide your own credential object that aligns with the `TokenCredential` class. For more details, see the [TokenCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.core.tokencredential) documentation.
-   If you do not provide a `token_credential` parameter, the library will use the Azure SDK's `DefaultAzureCredential` for authentication.

    -   Refer to the [Azure SDK](https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/credential-chains?tabs=dac#defaultazurecredential-overview) documentation for the order in which credential types are attempted.
    -   For local development with a User Principal Name (UPN), install either the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows) or the [Az.Accounts](https://www.powershellgallery.com/packages/Az.Accounts/2.2.3) PowerShell module.

    -   Note: When no credential is provided, the `DefaultAzureCredential` may select an unexpected identity. For example, if you log in to the Azure CLI with a Service Principal Name (SPN) but log in to Az.Accounts with a UPN, the `DefaultAzureCredential` will prioritize the CLI authentication.

## Installation

To install fabric-cicd, run:

```bash
pip install fabric-cicd
```

## Directory Structure

This library deploys from a directory containing files and directories committed via the Fabric Source Control UI. Ensure the `repository_directory` includes only these committed items, with the exception of the `parameter.yml` file.

```
/<your-directory>
    /<item-name>.<item-type>/
    /<item-name>.<item-type>/...
    /<item-name>.<item-type>/
    /<item-name>.<item-type>/...
    `/parameter.yml
```

## Changing Environment Specific Values

To handle environment-specific values committed to git, use a `parameter.yml` file. This file supports programmatically changing values based on the `environment` field in the `FabricWorkspace` class. If the environment value is not found in the `parameter.yml` file, any dependent replacements will be skipped.

Raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) for additional parameterization capabilities.

### find_replace

For generic find-and-replace operations. This will replace every instance of a specified string in every file. Specify the `find` value as the key and the `replace` value for each environment. See the [Example](example.md) page for a complete yaml file.

Note: A common use case for this function is to replace connection strings. I.e. find and replaced a connection guid referenced in data pipeline.

```yaml
find_replace:
    <find-this-value>:
        <environment-1>: <replace-with-this-value>
        <environment-2>: <replace-with-this-value>
```

### spark_pool

Environments attached to custom spark pools need to be parameterized because the `instance-pool-id` in the `Sparkcompute.yml` file isn't supported in the create/update environment APIs. Provide the `instance-pool-id` as the key, and the pool type and name as the values.

Environment parameterization(PPE/PROD) is not supported. If needed, raise a [feature request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml).

```yaml
spark_pool:
    <instance-pool-id>:
        type: <Capacity-or-Workspace>
        name: <pool-name>
```
