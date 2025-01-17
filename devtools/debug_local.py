# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug locally against the github repo

import sys
from pathlib import Path

from azure.identity import ClientSecretCredential

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))
from fabric_cicd import FabricWorkspace, change_log_level, publish_all_items, unpublish_all_orphan_items  # noqa F401

# change_log_level()

# The defined environment values should match the names found in the parameter.yml file
workspace_id = "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c"
environment = "dev"

# In this example, our workspace content sits within the root/sample/workspace directory
repository_directory = str(root_directory / "sample" / "workspace")

# Explicitly define which of the item types we want to deploy
item_type_in_scope = ["DataPipeline", "Notebook", "Environment", "SemanticModel", "Report"]


client_id = "your-client-id"
client_secret = "your-client-secret"
tenant_id = "your-tenant-id"
token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=environment,
    repository_directory=repository_directory,
    item_type_in_scope=item_type_in_scope,
    # Override base url in rare cases where it's different
    base_api_url="https://msitapi.fabric.microsoft.com/",
    # token_credential=token_credential,
)

# Publish all items defined in item_type_in_scope
# publish_all_items(target_workspace)

# Unpublish all items defined in scope not found in repository
# Excluding items with starting with DEBUG
# Because we're removing everything this gives the ability to "preserve" items
# unpublish_all_orphan_items(target_workspace, item_name_exclude_regex=r"^DEBUG.*")
