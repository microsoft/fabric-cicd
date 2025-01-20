fabric-cicd is a Python library designed for use with [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) workspaces. This library supports code-first Continuous Integration / Continuous Deployment (CI/CD) automations to seamlessly integrate Source Controlled workspaces into a deployment framework. The goal is to assist CI/CD developers who prefer not to interact directly with the Microsoft Fabric APIs.

## Base Expectations

-   Full deployment every time, without considering commit diffs
-   Deploys into the tenant of the executing identity

## Supported Item Types

The following item types are supported by the library:

-   Notebooks
-   Data Pipelines
-   Environments
-   Semantic Models
-   Reports

## Installation

To install fabric-cicd, run:

```bash
pip install fabric-cicd
```
While in private preview, to install fabric-cicd, run:

```bash
pip install --upgrade --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ fabric-cicd
```

## Basic Example

