# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug and call Fabric REST APIs locally from the github repo

from azure.identity import AzureCliCredential, AzurePowerShellCredential, ClientSecretCredential

from fabric_cicd import change_log_level, constants
from fabric_cicd._common._fabric_endpoint import FabricEndpoint
from fabric_cicd._common._validate_input import validate_token_credential

# Uncomment to enable debug
# change_log_level()

if __name__ == "__main__":
    # Azure CLI auth - comment out to use a different auth method
    token_credential = AzureCliCredential()

    # Uncomment to use PowerShell auth
    # token_credential = AzurePowerShellCredential()

    # Uncomment to use SPN auth
    # client_id = "your-client-id"
    # client_secret = "your-client-secret"
    # tenant_id = "your-tenant-id"
    # token_credential = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

    # Create endpoint object
    fe = FabricEndpoint(token_credential=validate_token_credential(token_credential))

    # Set workspace id variable if needed in API url
    workspace_id = "8f5c0cec-a8ea-48cd-9da4-871dc2642f4c"

    # API endpoint url (placeholder)
    api_url = f"{constants.DEFAULT_API_ROOT_URL}/v1/workspaces/{workspace_id}..."

    print("Making API call...")
    response = fe.invoke(
        method="POST",
        url=api_url,
        body={},
    )
    print("Call completed.")
