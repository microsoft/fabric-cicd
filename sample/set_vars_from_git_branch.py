# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example to set variables based on the target environment.
Environment is determined based on the current branch name.
"""
# START-EXAMPLE
from pathlib import Path

import git  # Depends on pip install gitpython

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# In this example, this file is being ran in the root/sample directory
root_directory = Path(__file__).resolve().parent.parent
repo = git.Repo(root_directory)
repo.remotes.origin.pull()
branch = repo.active_branch.name

# The defined environment values should match the names found in the parameter.yml file
if branch == "ppe":
    workspace_id = "a2745610-0253-4cf3-9e47-0b5cf8aa00f0"
    environment = "PPE"
elif branch == "main":
    workspace_id = "9010397b-7c0f-4d93-8620-90e51816e9e9"
    environment = "PROD"
else:
    raise ValueError("Invalid branch to deploy from")

# Sample values for FabricWorkspace parameters
repository_directory = "your-repository-directory"
item_type_in_scope = ["Notebook", "DataPipeline", "Environment"]

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
