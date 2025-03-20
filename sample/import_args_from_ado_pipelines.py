# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example of leveraging default authentication flows after authenticating in an Azure DevOps pipeline
Either as a service connection or a method like PowerShell

Shows how to gracefully pass through arguments added to a Python script task in Azure Pipelines
Like in the example shown in the post by Kevin Chant below:
https://www.kevinrchant.com/2025/03/11/operationalize-fabric-cicd-to-work-with-microsoft-fabric-and-azure-devops/
"""

# START-EXAMPLE
# argparse is required to gracefully deal with the arguments
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
import argparse

parser = argparse.ArgumentParser(description='Process Azure Pipeline arguments.')
parser.add_argument('--WorkspaceId', type=str)
parser.add_argument('--Environment', type=str)
parser.add_argument('--RepositoryDirectory', type=str)
parser.add_argument('--ItemsInScope', type=str)
args = parser.parse_args()

# Convert item_type_in_scope into a list
allitems = args.ItemsInScope
item_type_in_scope=allitems.split(",")
print(item_type_in_scope)

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id= args.WorkspaceId,
    environment=args.Environment,
    repository_directory=args.RepositoryDirectory,
    item_type_in_scope=item_type_in_scope,    
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)
