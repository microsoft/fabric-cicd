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

2. **Clone Your Forked Repository**:

    - Once the fork is complete, go to your GitHub account and open the forked repository.
    - Click on the **Code** button, then copy the URL (HTTPS or SSH).
    - Open your terminal and run the following command to clone your forked repository:

    ```sh
    git clone <URL-of-your-forked-repository>
    ```

3. **Create a Virtual Environment**:

    ```sh
    python -m venv venv
    ```

4. **Activate the Virtual Environment**:

    - On Windows:

        ```sh
        .\venv\Scripts\activate
        ```

    - On macOS and Linux:

        ```sh
        source venv/bin/activate
        ```

5. **Install the Dependencies**:

    ```sh
    pip install -r requirements.txt
    pip install -r requirements.dev.txt
    ```

6. **Open the Project in VS Code and Ensure the Virtual Environment is Selected**:

    - Open the Command Palette (Ctrl+Shift+P) and select `Python: Select Interpreter`.
    - Choose the interpreter from the `venv` directory.

7. **Ensure All VS Code Extensions Are Installed**:

    - Open the Command Palette (Ctrl+Shift+P) and select `Extensions: Show Recommended Extensions`.
    - Install all extensions recommended for the workspace.
