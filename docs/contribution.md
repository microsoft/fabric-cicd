# Contribution

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

## Prerequisites

Before you begin, ensure you have the following installed:

-   [Python](https://www.python.org/downloads/) (version 3.10 or higher)
-   [PowerShell](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
-   [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows) or [Az.Accounts PowerShell module](https://www.powershellgallery.com/packages/Az.Accounts/2.2.3)
-   [Visual Studio Code (VS Code)](https://code.visualstudio.com/)

## Initial Configuration

1. **Fork the Repository on GitHub**:

    - Go to the repository [fabric-cicd](https://github.com/microsoft/fabric-cicd) on GitHub.
    - In the top right corner, click on the **Fork** button.
    - This will create a copy of the repository in your own GitHub account.

1. **Clone Your Forked Repository**:

    - Once the fork is complete, go to your GitHub account and open the forked repository.
    - Click on the **Code** button, and clone to VS Code.

1. **Run activate.ps1**:

    - Open the Project in VS Code
    - Open PowerShell terminal
    - Run activate.ps1 which will install uv, and ruff if not already found. And set up the default environment leveraging uv sync.
        ```powershell
        .\activate.ps1
        ```
        _Note that this is technically optional and is designed to work with PowerShell. You can execute these steps manually as well, this is merely a helper_

1. **Select Python Interpreter**:

    - Open the Command Palette (Ctrl+Shift+P) and select `Python: Select Interpreter`.
    - Choose the interpreter from the `venv` directory.

1. **Ensure All VS Code Extensions Are Installed**:

    - Open the Command Palette (Ctrl+Shift+P) and select `Extensions: Show Recommended Extensions`.
    - Install all extensions recommended for the workspace.

## Development

### Managing Dependencies

-   All dependencies in this project are managed by uv which will resolve all dependencies and lock the versions to speed up virtual environment creation.
-   For additions, run:
    ```sh
    uv add <package-name>
    ```
-   For removals, run:
    ```sh
    uv remove <package-name>
    ```

### Code Formatting & Linting

-   The python code within this project is maintained by ruff.
-   If you install the recommended extensions, ruff will auto format on save of any file.
-   Before being able to merge a PR, ruff is ran in a Github Action to ensure the files are properly formatted and maintained.
-   To force linting, run the following.
    ```sh
    ruff format
    ruff check
    ```

### Pull Request Requirements

-   **All pull requests must be linked to an issue.** This ensures proper tracking and context for changes.
-   Before creating a pull request:
    1. **Create or identify an existing issue** that describes the problem, feature request, or change you're addressing.
    2. **Reference the issue** in your pull request description using one of these formats:
        - `Fixes #123` - for bug fixes
        - `Closes #456` - for implementing features  
        - `Resolves #789` - for other changes
    3. You can also reference issues in commit messages using the same format.
-   **GitHub Actions will automatically check** that your PR is linked to a valid issue and will fail if no valid reference is found.
-   If you need to create an issue, use the appropriate template:
    - [Bug Report](https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml)
    - [Feature Request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml) 
    - [Documentation](https://github.com/microsoft/fabric-cicd/issues/new?template=3-documentation.yml)
    - [Question](https://github.com/microsoft/fabric-cicd/issues/new?template=4-question.yml)
    - [Technical Debt](https://github.com/microsoft/fabric-cicd/issues/new?template=5-tech-debt.yml)
