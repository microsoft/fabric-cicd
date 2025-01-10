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
    ```

1. Open the project in VS Code and ensure the virtual environment is selected:

    - Open the Command Palette (Ctrl+Shift+P) and select `Python: Select Interpreter`.
    - Choose the interpreter from the venv directory.

1. Ensure all VS Code extensions are installed:

    - Open the Command Palette (Ctrl+Shift+P) and select `Extensions: Show Recommended Extensions`.
    - Install all extensions recommended for the workspace
