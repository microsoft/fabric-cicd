# Fabric CICD
[![PyPI version](https://badge.fury.io/py/fabric-cicd.svg)](https://badge.fury.io/py/fabric-cicd)
[![Read The Docs](https://readthedocs.org/projects/fabric-cicd/badge/?version=0.1&style=flat)](https://readthedocs.org/projects/fabric-cicd/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Downloads](https://static.pepy.tech/badge/fabric-cicd)](https://pepy.tech/project/fabric-cicd)

---
[Read the documentation on ReadTheDocs!](https://fabric-cicd.readthedocs.io/en/stable/)
---

Fabric CICD is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library is intended to support code first CICD integrations to seamlessly integrate Source Controlled workspaces into a deployment framework.  The goal of this library is to support the CICD developers that don't want to dig into the weeds of interacting directly with the Microsoft Fabric APIs.  

If you encounter any issues, please [raise a bug](https://github.com/microsoft/fabric-cicd/issues/new?assignees=&labels=&projects=&template=bug_report.md&title=).

If you have ideas for new features/functions, please [request a feature](https://github.com/microsoft/fabric-cicd/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=).

## Featured Scenarios
- Deploying Items hosted in a repository

### In Scope Item Types
  - Notebooks
  - Data Pipelines
  - Environments
  
### Limitations
  - Notebooks Limitations
    - Attached lakehouses are not changed during the deployment
    - Attached resources are not included in the deployment
  - Data Pipelines
    - Connections are not changed during the deployment
  - Environments
    - Custom and Public Libraries are not included in the deployment
    
## How to Use
```python
pip install fabric-cicd

from fabric-cicd.FabricWorkspace import FabricWorkspace

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id='',
    environment='',
    repository_directory=r'',
    item_type_in_scope=[],
    debug_output=False
)

# Publish all items defined in scope
target_workspace.publish_all_items()
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
