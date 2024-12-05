# Update the local repository with the latest changes
git pull

# Determine the current branch and set the corresponding workspace ID
$current_branch = git branch --show-current

# Map the current branch to its corresponding workspace ID
switch ($current_branch) {
    "Develop" { $workspace_id = "fd844302-8fdd-42c1-b1ff-e35bc8e294e4"; $environment = "dev" }
    "Test"    { $workspace_id = "de794e10-4e1a-48ba-9a15-d151a4f16dcb"; $environment = "test" }
    "Main"    { $workspace_id = "aac1b554-e482-435e-95dc-266c1db7200b"; $environment = "main" }
    default   { Throw "Invalid branch to deploy from" }
}

$repository_directory = Resolve-Path -Path "$PSScriptRoot\..\..\workspace\HelixFabric-Engineering"
$item_type_in_scope = "['DataPipeline', 'Notebook', 'Environment']"

# Check if an Azure context is available. If not, connect to Azure.
if (-not (Get-AzContext)) {
    Connect-AzAccount
}

# Set the python path environment variable for relative module references
$env:PYTHONPATH = ".;$PSScriptRoot"

# Execute a Python script for deployment tasks
Python -c @"
import deployfabric.install_requirements
from deployfabric.FabricWorkspace import FabricWorkspace
from helixCustom.helixCustomFunctions import preprocess_all_items

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id='$workspace_id',
    environment='$environment',
    repository_directory=r'$repository_directory',
    item_type_in_scope=$item_type_in_scope,
    base_api_url='https://msitapi.fabric.microsoft.com/',
    debug_output=$False
)

# Preprocess items before publishing/unpublishing
preprocess_all_items(target_workspace)

# Publish all items defined in scope
target_workspace.publish_all_items()

# Unpublish all items defined in scope not found in repository
target_workspace.unpublish_all_orphan_items(item_name_exclude_regex=r'^DEBUGING.*')
"@

# Reset changes in the workspace directory to ensure a clean state
git restore --source=HEAD --staged --worktree $repository_directory