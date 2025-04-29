# Release Pipeline Examples

The following are some common examples of how to deploy from tooling like Azure DevOps and GitHub. Note that this is not an exhaustive list, nor is it a recommendation to not use a proper Build/Release stage. These are simplified to show the potential.

## Azure CLI

This approach will work for both the Default Credential Flow and the Azure CLI Credential Flow. However, it is recommended to use the Azure CLI Credential Flow in case there are multiple identities present in the build VM.

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

    ```yaml
    ###Unconfirmed example at this time, however, the Azure DevOps example is a good starting point
    ```

## Azure PowerShell

This approach will work for both the Default Credential Flow and the Azure PowerShell Credential Flow. However, it is recommended to use the Azure PowerShell Credential Flow in case there are multiple identities present in the build VM.

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

    ```yaml
    ###Unconfirmed example at this time, however, the Azure DevOps example is a good starting point
    ```

## Variable Groups

This approach is best suited for the Passed Arguments example found in the Deployment Variable Examples, in combination with the Explicit SPN Credential flow in the Authentication Examples. The goal being to define values within the pipeline (or outside the pipeline in Azure DevOps variable groups) and inject them into the python script. Note this also doesn't take a dependency on PowerShell for those organizations or scenarios that PowerShell is not allowed.
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
                  arguments: |
                    --spn_client_id $(client_id) # from Fabric_Deployment_Group_KeyVault
                    --spn_client_secret $(client_secret) # from Fabric_Deployment_Group_KeyVault
                    --tenant_id $(tenant_id) # from Fabric_Deployment_Group_KeyVault
                    --workspace_id $(workspace_id) # from Fabric_Deployment_Group
                    --environment $(environment_name) # from Fabric_Deployment_Group
                    --repository_directory $repository_directory # from Fabric_Deployment_Group
                    --item_types_in_scope ${{ parameters.items_in_scope }}
    ```

=== "GitHub"

    ```yaml
    ###Unconfirmed example at this time, however, the Azure DevOps example is a good starting point
    ```
