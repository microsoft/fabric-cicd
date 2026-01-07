# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug and call Fabric REST APIs locally from the github repo

from azure.identity import DefaultAzureCredential

from fabric_cicd import change_log_level, constants
from fabric_cicd._common._fabric_endpoint import FabricEndpoint

# Uncomment to enable debug
# change_log_level()

if __name__ == "__main__":
    fe = FabricEndpoint(DefaultAzureCredential())

    print("Making API call...")

    # Set workspace id variable if needed in API url
    workspace_id = "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c"

    # url placeholder
    api_url = f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}..."

    response = fe.invoke(
        method="POST",
        url=api_url,
        body={},
    )

    print("Call completed.")
