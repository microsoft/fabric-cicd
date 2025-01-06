# Fabric CICD

[![Language](https://img.shields.io/badge/language-Python-blue.svg)](https://www.python.org/)
[![TestPyPI version](https://img.shields.io/badge/TestPyPI-fabric--cicd-blue)](https://test.pypi.org/project/fabric-cicd/)
[![Read The Docs](https://readthedocs.org/projects/fabric-cicd/badge/?version=latest&style=flat)](https://readthedocs.org/projects/fabric-cicd/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/charliermarsh/ruff)

<!-- [![PyPI version](https://badge.fury.io/py/fabric-cicd.svg)](https://badge.fury.io/py/fabric-cicd) -->
<!-- [![Downloads](https://static.pepy.tech/badge/fabric-cicd)](https://pepy.tech/project/fabric-cicd) -->

[Read full documentation on ReadTheDocs](https://fabric-cicd.readthedocs.io/en/latest/)

---

## Project Overview

Fabric CICD is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library is intended to support code first CICD integrations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal of this library is to support the CICD developers that don't want to dig into the weeds of interacting directly with the Microsoft Fabric APIs.
Fabric CICD is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library is intended to support code first CICD integrations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal of this library is to support the CICD developers that don't want to dig into the weeds of interacting directly with the Microsoft Fabric APIs.

If you encounter any issues, please [raise a bug](https://github.com/microsoft/fabric-cicd/issues/new?assignees=&labels=&projects=&template=bug_report.md&title=).

If you have ideas for new features/functions, please [request a feature](https://github.com/microsoft/fabric-cicd/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=).

### Featured Scenarios

- Deploying Items hosted in a repository
- 100% deployment, does not consider diffs
- Deploys into the tenant of the executing identity

### In Scope Item Types

- Notebooks
- Data Pipelines
- Environments


- Notebooks
- Data Pipelines
- Environments

### Limitations

- **Notebooks**:
  - Attached lakehouses are not changed during deployment
  - Attached resources are not included in deployment
- **Data Pipelines**:
  - Connections are not changed during deployment
- **Environments**:
  - Custom and public libraries are not included in deployment
- **Folders**:
  - Sub folders are not included in deployment

### Authentication

- Optionally provide your own credential object aligned with the [TokenCredential class](https://learn.microsoft.com/en-us/dotnet/api/azure.core.tokencredential) in the `token_credential` parameter.
- If the `token_credential` parameter is omitted, the library uses the Azure SDK `DefaultAzureCredential` for authentication.
  - Refer to the [Azure SDK documentation](https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/credential-chains?tabs=dac#defaultazurecredential-overview) for the order in which credential types are attempted.
  - For local development with a UPN, install either the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows) or [Az.Accounts](https://www.powershellgallery.com/packages/Az.Accounts/2.2.3).
  - Note: When no credential is provided, the `DefaultAzureCredential` may choose an unexpected identity. For instance, if you log in to the Azure CLI with an SPN but log in to Az.Account with a UPN, the `DefaultAzureCredential` will choose the CLI authentication first.

## Contribute

This project welcomes contributions and suggestions. Most contributions require you to agree to a Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us the rights to use your contribution. For details, visit [https://cla.opensource.microsoft.com](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

### Requirements

Before you begin, ensure you have the following installed:

- [Python](https://www.python.org/downloads/) (version 3.10 or higher)
- [PowerShell](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows) or [Az.Accounts PowerShell module](https://www.powershellgallery.com/packages/Az.Accounts/2.2.3)
- [Visual Studio Code (VS Code)](https://code.visualstudio.com/)

### Initial Configuration

1. Clone the repository:

   ```sh
   git clone https://github.com/microsoft/fabric-cicd.git /your/target/directory
   cd /your/target/directory
   ```

1. Create a virtual environment:

   ```sh
   python -m venv venv
   ```

1. Activate the virtual environment:

   - On Windows:

     ```sh
     .\venv\Scripts\activate
     ```

   - On macOS and Linux:

     ```sh
     source venv/bin/activate
     ```

1. Install the dependencies:

   ```sh
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

1. Open the project in VS Code and ensure the virtual environment is selected:

- Open the Command Palette (Ctrl+Shift+P) and select `Python: Select Interpreter`.
- Choose the interpreter from the venv directory.

## Support

### How to file issues and get help

This project uses GitHub Issues to track bugs, feature requests, and questions. Please search the existing issues before filing new issues to avoid duplicates. For new issues, file your bug, feature request, or question as a new Issue.

### Microsoft Support Policy

Support for this **PROJECT or PRODUCT** is limited to the resources listed above.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
