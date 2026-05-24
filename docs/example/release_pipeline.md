# Release Pipeline Examples

The following are some common examples of how to deploy from tooling like Azure DevOps and GitHub. Note that this is not an exhaustive list, nor is it a recommendation to not use a proper Build/Release stage. These are simplified to show the potential.

## Azure CLI

This approach uses the Azure CLI Credential Flow. An explicit credential method is required. This avoids ambiguity when multiple identities are present in the build VM.

=== "Azure DevOps"

    ```yml
    trigger:
      branches:
        include:
          - dev
          - main
    stages:
      - stage: Build_Release
        jobs:
          - job: Build
            pool:
              vmImage: windows-latest
            steps:
              - checkout: self
              - task: UsePythonVersion@0
                inputs:
                  versionSpec: '3.12'
                  addToPath: true
              - script: |
                  pip install fabric-cicd
                displayName: 'Install fabric-cicd'
              - task: AzureCLI@2
                displayName: "Deploy Fabric Workspace"
                inputs:
                  azureSubscription: "your-service-connection"
                  scriptType: "ps"
                  scriptLocation: "inlineScript"
                  inlineScript: |
                    python -u $(System.DefaultWorkingDirectory)/.deploy/fabric_workspace.py
    ```

=== "GitHub"

    This example uses [workload identity federation (OIDC)](https://learn.microsoft.com/en-us/entra/workload-id/workload-identity-federation) for authentication. You must configure a federated identity credential on your Azure AD app registration that trusts GitHub's OIDC token issuer. See [Azure login with OIDC](https://github.com/azure/login#login-with-openid-connect-oidc-recommended) for setup instructions.

    ```yaml
    name: Deploy Fabric Workspace

    on:
      push:
        branches:
          - dev
          - main

    permissions:
      id-token: write
      contents: read

    jobs:
      deploy:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v4
          - uses: actions/setup-python@v5
            with:
              python-version: '3.12'
          - run: pip install fabric-cicd
          - uses: azure/login@v2
            with:
              client-id: ${{ secrets.AZURE_CLIENT_ID }}
              tenant-id: ${{ secrets.AZURE_TENANT_ID }}
              subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
          - run: python .deploy/fabric_workspace.py
    ```

## Azure PowerShell

This approach uses the Azure PowerShell Credential Flow. An explicit credential method is required. This avoids ambiguity when multiple identities are present in the build VM.

=== "Azure DevOps"

    ```yml
    trigger:
      branches:
        include:
          - dev
          - main
    stages:
      - stage: Build_Release
        jobs:
          - job: Build
            pool:
              vmImage: windows-latest
            steps:
              - checkout: self
              - task: UsePythonVersion@0
                inputs:
                  versionSpec: '3.12'
                  addToPath: true
              - script: |
                  pip install fabric-cicd
                displayName: 'Install fabric-cicd'
              - task: AzurePowerShell@5
                displayName: "Deploy Fabric Workspace"
                inputs:
                  azureSubscription: "your-service-connection"
                  scriptType: "InlineScript"
                  scriptLocation: "inlineScript"
                  pwsh: true
                  Inline: |
                    python -u $(System.DefaultWorkingDirectory)/.deploy/fabric_workspace.py
    ```

=== "GitHub"

    This example uses [workload identity federation (OIDC)](https://learn.microsoft.com/en-us/entra/workload-id/workload-identity-federation) with `enable-AzPSSession: true` to set up an Azure PowerShell context. You must configure a federated identity credential on your Azure AD app registration. See [Azure login with OIDC](https://github.com/azure/login#login-with-openid-connect-oidc-recommended) for setup instructions.

    ```yaml
    name: Deploy Fabric Workspace

    on:
      push:
        branches:
          - dev
          - main

    permissions:
      id-token: write
      contents: read

    jobs:
      deploy:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v4
          - uses: actions/setup-python@v5
            with:
              python-version: '3.12'
          - run: pip install fabric-cicd
          - uses: azure/login@v2
            with:
              client-id: ${{ secrets.AZURE_CLIENT_ID }}
              tenant-id: ${{ secrets.AZURE_TENANT_ID }}
              subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
              enable-AzPSSession: true
          - run: python .deploy/fabric_workspace.py
    ```

## Variable Groups

This approach is best suited for the Passed Arguments example found in the Deployment Variable Examples, in combination with a `ClientSecretCredential` as shown in the [Authentication Examples](authentication.md). The goal is to define values within the pipeline (or outside the pipeline in Azure DevOps variable groups) and inject them into the python script. Note this also doesn't take a dependency on PowerShell for those organizations or scenarios where PowerShell is not allowed.
=== "Azure DevOps"

    ```yml
    trigger:
      branches:
        include:
          - dev
          - main

    parameters:
    - name: items_in_scope
      displayName: Enter Fabric items to be deployed
      type: string
      default: '["Notebook","DataPipeline","Environment"]'

    variables:
    - group: Fabric_Deployment_Group_KeyVault # Linked to Azure Key Vault and contains tenant id, SPN client id, and SPN secret
    - group: Fabric_Deployment_Group  # Contains workspace_name and repository directory name

    stages:
      - stage: Build_Release
        jobs:
          - job: Build
            pool:
              vmImage: windows-latest
            steps:
              - checkout: self
              - task: UsePythonVersion@0
                inputs:
                  versionSpec: '3.12'
                  addToPath: true
              - script: |
                  pip install fabric-cicd
                displayName: 'Install fabric-cicd'
              - task: PythonScript@0
                inputs:
                  scriptSource: 'filePath'
                  scriptPath: '.deploy/fabric_workspace.py'
                  arguments: >-
                    --spn_client_id $(client_id) # from Fabric_Deployment_Group_KeyVault
                    --spn_client_secret $(client_secret) # from Fabric_Deployment_Group_KeyVault
                    --tenant_id $(tenant_id) # from Fabric_Deployment_Group_KeyVault
                    --workspace_id $(workspace_id) # from Fabric_Deployment_Group
                    --environment $(environment_name) # from Fabric_Deployment_Group
                    --repository_directory $(repository_directory) # from Fabric_Deployment_Group
                    --item_types_in_scope ${{ parameters.items_in_scope }}
    ```

=== "GitHub"

    ```yaml
    name: Deploy Fabric Workspace

    on:
      push:
        branches:
          - dev
          - main

    jobs:
      deploy:
        runs-on: ubuntu-latest
        environment: ${{ github.ref_name }}  # Maps to GitHub Environment for secrets/variables
        steps:
          - uses: actions/checkout@v4
          - uses: actions/setup-python@v5
            with:
              python-version: '3.12'
          - run: pip install fabric-cicd
          - run: |
              python .deploy/fabric_workspace.py \
                --spn_client_id ${{ secrets.SPN_CLIENT_ID }} \
                --spn_client_secret ${{ secrets.SPN_CLIENT_SECRET }} \
                --tenant_id ${{ secrets.TENANT_ID }} \
                --workspace_id ${{ vars.WORKSPACE_ID }} \
                --environment ${{ vars.ENVIRONMENT_NAME }} \
                --repository_directory ${{ vars.REPOSITORY_DIRECTORY }} \
                --item_types_in_scope "${{ vars.ITEMS_IN_SCOPE }}"
    ```
