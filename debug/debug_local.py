# The following is intended for developers of fabric-cicd to debug locally against the github repo

from pathlib import Path
import sys

root_directory = Path(__file__).resolve().parent.parent
sys.path.append(str(root_directory / "src"))
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# The defined environment values should match the names found in the parameter.yml file
workspace_id = "fd844302-8fdd-42c1-b1ff-e35bc8e294e4"
environment = "dev"

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
    base_api_url="https://msitapi.fabric.microsoft.com/",
    # Print all api calls to debug what is being passed to fabric
    # debug_output=True,
)

# Publish all items defined in item_type_in_scope
# publish_all_items(target_workspace)

# Unpublish all items defined in scope not found in repository
# Excluding items with starting with DEBUG
# Because we're removing everything this gives the ability to "preserve" items
# unpublish_all_orphan_items(target_workspace, item_name_exclude_regex=r"^DEBUG.*")
