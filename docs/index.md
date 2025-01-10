# Getting Started

Fabric CICD is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library is intended to support code-first CICD integrations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal of this library is to support the CICD developers that don't want to dig into the weeds of interacting directly with the Microsoft Fabric APIs.

## Base Expectations

-   Full deployment every time, does not consider commit diffs
-   Deploys into the tenant of the executing identity

## Supported Item Types

The following Item Types are supported by the library:

-   Notebooks
-   Data Pipelines
-   Environments

## Authentication

-   You can optionally provide your own credential object that aligns with the `TokenCredential` class. For more details, see the [TokenCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.core.tokencredential).
-   If you do not provide a `token_credential` parameter, the library will use the Azure SDK's `DefaultAzureCredential` for authentication.

    -   Refer to the [Azure SDK](https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/credential-chains?tabs=dac#defaultazurecredential-overview) documentation for the order in which credential types are attempted.
    -   For local development with a User Principal Name (UPN), install either the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows) or the [Az.Accounts](https://www.powershellgallery.com/packages/Az.Accounts/2.2.3) PowerShell module.

    -   Note: When no credential is provided, the `DefaultAzureCredential` may select an unexpected identity. For example, if you log in to the Azure CLI with a Service Principal Name (SPN) but log in to Az.Accounts with a UPN, the `DefaultAzureCredential` will prioritize the CLI authentication.

## Installation

To install fabric-cicd, run:

```bash
pip install fabric-cicd
```
