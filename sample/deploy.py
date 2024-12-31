"""
The following sample can be ran locally, or in Azure DevOps Release task
Define the deploy type to change the behavior
When ADO is defined, Authentication is passed in from devops task and 
environment is determined based on preceeding build branch
When local is defined, Authentication is passed in from windows identity and 
environment is determined based on the current branch name
"""

deployType = "local"  # ["local", "azuredevops"]

from pathlib import Path
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# In this example, this file is being ran in the root/sample directory
root_directory = Path(__file__).resolve().parent.parent

if deployType == "azuredevops":
    import os

    branch = os.getenv("BUILD_SOURCEBRANCHNAME")
else:
    import git

    repo = git.Repo(root_directory)
    repo.remotes.origin.pull()
    branch = repo.active_branch.name

# The defined environment values should match the names found in the parameter.yml file
if branch == "Develop":
    workspace_id = "a2745610-0253-4cf3-9e47-0b5cf8aa00f0"
    environment = "dev"
elif branch == "Test":
    workspace_id = "b33ecfe9-a9ad-4aca-ad9c-72c0a728f2c0"
    environment = "test"
elif branch == "Main":
    workspace_id = "9010397b-7c0f-4d93-8620-90e51816e9e9"
    environment = "main"
else:
    raise ValueError("Invalid branch to deploy from")

# In this example, our workspace content sits within the root/sample/workspace directory
repository_directory = str(root_directory / "sample" / "workspace")

# Explicitly define which of the item types we want to deploy
item_type_in_scope = ["DataPipeline", "Notebook", "Environment"]

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    # Override base url in rare cases where it's different
    # base_api_url='https://msitapi.fabric.microsoft.com/',
    # Print all api calls to debug what is being passed to fabric
    # debug_output=True,
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
# Excluding items with starting with the name DEBUG in the workspace
unpublish_all_orphan_items(target_workspace, item_name_exclude_regex=r"^DEBUG.*")
