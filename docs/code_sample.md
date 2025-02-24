# Code Samples

## Authentication

The following are the most common authentication flows for fabric-cicd. However, because fabric-cicd supports any [TokenCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.core.tokencredential), there are multiple authentication methods available beyond the ones described here.

### Default Credentials

This approach utilizes the default credential flow, meaning no explicit TokenCredential is provided. It is the most common authentication method and is particularly useful when integrating with Azure DevOps. For local development, ensure that you are logged in using either the Azure CLI (az login) or the Azure PowerShell module (Connect-AzAccount).

```python
{% include "../sample/auth_default_credential.py" start="# START-EXAMPLE"%}
```

### SPN Credentials

This method explicitly utilizes a Service Principal Name (SPN) with a Secret credential flow, leveraging the ClientSecretCredential class. Organizations permitted to use SPN + Secret credentials may also combine this approach with Azure Key Vault to securely retrieve the secret.

```python
{% include "../sample/auth_spn_secret.py" start="# START-EXAMPLE"%}
```

## Deployment Variables

A key concept in CI/CD is defining environment-specific deployment variables. At minimum, a Workspace Id needs to be defined for every branch you intend to deploy from. Additionally, if leveraging the parameter.yml file, a target environment name is also required.

### Azure DevOps Release

When deploying from an Azure DevOps Release, the `BUILD_SOURCEBRANCHNAME` environment variable determines the target environment. This approach is also compatible with composite YAML-based build and release pipelines.

```python
{% include "../sample/set_vars_from_ado_build.py" start="# START-EXAMPLE"%}
```

### Local GIT Branch

When deploying from a local machine, the script identifies the target environment based on the active Git branch. The script fetches the latest updates from the remote repository before determining the branch name. Ensure that the required dependency (gitpython) is installed before running the script.

```python
{% include "../sample/set_vars_from_git_branch.py" start="# START-EXAMPLE"%}
```
