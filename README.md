# Fabric CICD

[![Language](https://img.shields.io/badge/language-Python-blue.svg)](https://www.python.org/)
[![PyPi version](https://badgen.net/pypi/v/fabric-cicd/)](https://pypi.org/project/fabric-cicd)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/charliermarsh/ruff)
[![Tests](https://img.shields.io/github/actions/workflow/status/microsoft/fabric-cicd/test.yml?logo=github&label=tests&branch=main)](https://github.com/microsoft/fabric-cicd/actions/workflows/test.yml)

---

## Project Overview

fabric-cicd is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library supports code-first Continuous Integration / Continuous Deployment (CI/CD) automations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal is to assist CI/CD developers who prefer not to interact directly with the Microsoft Fabric APIs.

## Supported Item Types

fabric-cicd supports various Microsoft Fabric item types with different capabilities. The table below shows what features are supported for each item type:

| Item Type | Source Control Support | Parameterization (find_replace) | Content Deployment | Connection Management Required | Unpublish Support | Manual Initial Configuration | Supports Ordered Deployment |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DataPipeline |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✗  |  ✗  |
| Environment |  ✓  |  ✗  |  ✗  |  ✗  |  ✓  |  ✗  |  ✗  |
| Notebook |  ✓  |  ✓  |  ✗  |  ✗  |  ✓  |  ✗  |  ✗  |
| Report |  ✓  |  ✓  |  ✓  |  ✗  |  ✓  |  ✗  |  ✗  |
| SemanticModel |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✗  |
| Lakehouse |  ✓  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |
| MirroredDatabase |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✗  |
| VariableLibrary |  ✓  |  ✓  |  ✓  |  ✗  |  ✓  |  ✗  |  ✗  |
| CopyJob |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✗  |
| Eventhouse |  ✓  |  ✗  |  ✓  |  ✗  |  ✓  |  ✗  |  ✗  |
| KQLDatabase |  ✓  |  ✗  |  ✗  |  ✗  |  ✓  |  ✗  |  ✗  |
| KQLQueryset |  ✓  |  ✓  |  ✓  |  ✗  |  ✓  |  ✗  |  ✗  |
| Reflex |  ✓  |  ✗  |  ✓  |  ✗  |  ✓  |  ✗  |  ✗  |
| Eventstream |  ✓  |  ✓  |  ✓  |  ✗  |  ✓  |  ✓  |  ✗  |
| Warehouse |  ✓  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |
| SQLDatabase |  ✓  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |  ✗  |
| KQLDashboard |  ✓  |  ✓  |  ✓  |  ✗  |  ✓  |  ✗  |  ✗  |
| Dataflow |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
| GraphQLApi |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✗  |  ✗  |

> **Legend**: ✓ = Supported/Required, ✗ = Not supported/Not required
>
> **Notes**: 
> - **Source Control Support**: All listed item types support source control integration
> - **Parameterization**: Support for `find_replace` section in `parameter.yml` for environment-specific values
> - **Content Deployment**: Whether the item's content/data is deployed (vs. shell-only deployment)
> - **Connection Management**: Whether manual connection setup/configuration is required
> - **Unpublish Support**: Whether the item supports automatic unpublishing of orphaned items
> - **Manual Initial Configuration**: Whether manual configuration steps are required after initial deployment
> - **Ordered Deployment**: Whether the system automatically handles deployment order for dependencies

For detailed information about each item type and specific configuration requirements, see the [full documentation](https://microsoft.github.io/fabric-cicd/latest/how_to/item_types/).

## Documentation

All documentation is hosted on our [fabric-cicd](https://microsoft.github.io/fabric-cicd/) GitHub Pages

Section Overview:

-   [Home](https://microsoft.github.io/fabric-cicd/latest/)
-   [How To](https://microsoft.github.io/fabric-cicd/latest/how_to/)
-   [Examples](https://microsoft.github.io/fabric-cicd/latest/example/)
-   [Contribution](https://microsoft.github.io/fabric-cicd/latest/contribution/)
-   [Changelog](https://microsoft.github.io/fabric-cicd/latest/changelog/)
-   [About](https://microsoft.github.io/fabric-cicd/latest/help/) - Inclusive of Support & Security Policies

## Installation

To install fabric-cicd, run:

```bash
pip install fabric-cicd
```

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
