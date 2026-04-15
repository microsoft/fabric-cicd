"""
Unified Fabric CI/CD script: create workspaces, connect Git, and deploy.

Usage:
    python fabriccicd.py create       - Create dev/test/prod workspaces and assign roles
    python fabriccicd.py connect      - Connect DEV workspace to Azure DevOps Git
    python fabriccicd.py deploy [ENV] - Deploy items to a workspace (default: DEV)
    python fabriccicd.py all [ENV]    - Run all steps in order (default: DEV)
"""

import sys
import time

import requests
from azure.identity import AzureCliCredential

# ---------- Configuration ----------
CAPACITY_ID = "F41BC187-38C5-4835-817C-629BD784ADD7"
SECURITY_GROUP_ID = "cbb157e6-143f-4eb7-a9fb-688199a3b569"
REPO_PATH = r"C:\Users\v-vijareddy\Asimov-vNext-Deployment\fabric"
BASE_URL = "https://api.fabric.microsoft.com/v1"

WORKSPACES = {
    "DEV": "baae0a18-6e4f-4401-ad14-99a3beb88be1",
    "TEST": "cf788766-a085-4819-8a97-04905f54a1ea",
    "PROD": "9bfa024f-ce51-4ce8-aea2-f26931a4d449",
}

GIT_CONFIG = {
    "gitProviderType": "AzureDevOps",
    "organizationName": "msazure",
    "projectName": "One",
    "repositoryName": "Asimov-vNext-Deployment",
    "branchName": "dev",
    "directoryName": "fabric",
}

DEV_WORKSPACE_ID = WORKSPACES["DEV"]
DEV_WORKSPACE_NAME = "fabric-cicd-dev"


# ---------- Helpers ----------
def log(message, indent=False):
    prefix = "  -> " if indent else ">> "
    print(f"{prefix}{message}")


def get_headers():
    log("Signing in with Azure CLI credentials...")
    cred = AzureCliCredential()
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    log("Authenticated successfully.", indent=True)
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def poll_long_running_operation(headers, response):
    operation_url = response.headers.get("Location")
    if not operation_url:
        operation_id = response.headers.get("x-ms-operation-id")
        if operation_id:
            operation_url = f"{BASE_URL}/operations/{operation_id}"
    if not operation_url:
        return None

    retry_after = int(response.headers.get("Retry-After", 30))
    while True:
        time.sleep(retry_after)
        poll_resp = requests.get(operation_url, headers=headers)
        if poll_resp.status_code != 200:
            log(f"Polling error: {poll_resp.status_code} - {poll_resp.text}", indent=True)
            return poll_resp
        result = poll_resp.json()
        status = result.get("status", "")
        log(f"Status: {status}", indent=True)
        if status in ("Succeeded", "Failed", "Cancelled"):
            return poll_resp


# ---------- Create Workspaces ----------
def create_workspaces(headers):
    print("\n" + "=" * 60)
    print("  Create Workspaces")
    print("=" * 60 + "\n")

    for env in ["dev", "test", "prod"]:
        workspace_name = f"fabric-cicd-{env}"

        resp = requests.post(
            f"{BASE_URL}/workspaces",
            headers=headers,
            json={"displayName": workspace_name, "capacityId": CAPACITY_ID},
        )

        if resp.status_code == 201:
            workspace_id = resp.json().get("id")
            print(f"{env.upper()}: Workspace created! ID: {workspace_id}")
        elif resp.status_code == 409:
            lookup = requests.get(
                f"{BASE_URL}/workspaces?$filter=displayName eq '{workspace_name}'",
                headers=headers,
            )
            workspace_id = lookup.json().get("value", [{}])[0].get("id")
            print(f"{env.upper()}: Workspace already exists. ID: {workspace_id}")
        else:
            print(f"{env.upper()}: {resp.status_code} - {resp.json()}")
            continue

        role_resp = requests.post(
            f"{BASE_URL}/workspaces/{workspace_id}/roleAssignments",
            headers=headers,
            json={
                "principal": {"id": SECURITY_GROUP_ID, "type": "Group"},
                "role": "Contributor",
            },
        )
        if role_resp.status_code == 201:
            print("  -> Security group assigned as Contributor")
        else:
            print(f"  -> Role assignment: {role_resp.status_code} - {role_resp.json()}")


# ---------- Connect Git ----------
def connect_git(headers):
    print("\n" + "=" * 60)
    print("  Connect DEV Workspace to Azure DevOps Git")
    print("=" * 60 + "\n")

    # Step 1: Connect
    log(f"Step 1/3: Connecting workspace '{DEV_WORKSPACE_NAME}' to Git repo...")
    log(
        f"Repo: {GIT_CONFIG['organizationName']}/{GIT_CONFIG['projectName']}/{GIT_CONFIG['repositoryName']}",
        indent=True,
    )
    log(f"Branch: {GIT_CONFIG['branchName']}  |  Directory: /{GIT_CONFIG['directoryName']}", indent=True)

    url = f"{BASE_URL}/workspaces/{DEV_WORKSPACE_ID}/git/connect"
    resp = requests.post(url, headers=headers, json={"gitProviderDetails": GIT_CONFIG})

    if resp.status_code == 200:
        log("Connected successfully!", indent=True)
    else:
        error = resp.json() if resp.text else {}
        if error.get("errorCode") == "WorkspaceAlreadyConnectedToGit":
            log("Already connected to Git - skipping.", indent=True)
        else:
            log(f"Failed to connect: {error.get('message', resp.text)}", indent=True)
            return False
    print()

    # Step 2: Initialize
    log("Step 2/3: Initializing Git connection...")
    url = f"{BASE_URL}/workspaces/{DEV_WORKSPACE_ID}/git/initializeConnection"
    resp = requests.post(url, headers=headers, json={"initializationStrategy": "PreferRemote"})

    if resp.status_code == 200:
        result = resp.json()
        action = result.get("requiredAction", "None")
        log(f"Initialized. Next action: {action}", indent=True)
    elif resp.status_code == 202:
        log("Initialization in progress, waiting...", indent=True)
        poll_long_running_operation(headers, resp)
        action, result = "None", {}
    else:
        log(f"Failed to initialize: {resp.text}", indent=True)
        return False
    print()

    # Step 3: Sync
    if action == "UpdateFromGit":
        log("Step 3/3: Syncing workspace from Git (pulling latest)...")
        url = f"{BASE_URL}/workspaces/{DEV_WORKSPACE_ID}/git/updateFromGit"
        body = {
            "remoteCommitHash": result.get("remoteCommitHash", ""),
            "conflictResolution": {"conflictResolutionType": "Workspace", "conflictResolutionPolicy": "PreferRemote"},
        }
        if result.get("workspaceHead"):
            body["workspaceHead"] = result["workspaceHead"]

        resp = requests.post(url, headers=headers, json=body)
        if resp.status_code == 200:
            log("Workspace synced from Git!", indent=True)
        elif resp.status_code == 202:
            log("Sync in progress, waiting...", indent=True)
            poll_long_running_operation(headers, resp)
        else:
            log(f"Sync failed: {resp.text}", indent=True)
    elif action == "CommitToGit":
        log("Step 3/3: Workspace has local changes that need to be committed to Git.", indent=True)
        log("Run a commit-to-git operation to push workspace changes.", indent=True)
    else:
        log("Step 3/3: No sync needed - workspace and Git are already in sync.", indent=True)

    print("\n" + "=" * 60)
    print("  DEV workspace is connected to Git.")
    print("=" * 60 + "\n")
    return True


# ---------- Deploy ----------
def deploy(env):
    from fabric_cicd import FabricWorkspace, publish_all_items

    print("\n" + "=" * 60)
    print(f"  Deploy to {env}")
    print("=" * 60 + "\n")

    if env not in WORKSPACES:
        print(f"Invalid environment '{env}'. Choose from: {', '.join(WORKSPACES.keys())}")
        sys.exit(1)

    creds = AzureCliCredential()
    workspace = FabricWorkspace(
        workspace_id=WORKSPACES[env],
        environment=env,
        repository_directory=REPO_PATH,
        token_credential=creds,
    )
    print(f"Deploying to {env}...")
    publish_all_items(workspace)
    print("Done!")


# ---------- Main ----------
def main():
    usage = (
        "Usage:\n"
        "  python fabriccicd.py create       - Create workspaces\n"
        "  python fabriccicd.py connect      - Connect DEV to Git\n"
        "  python fabriccicd.py deploy [ENV] - Deploy (default: DEV)\n"
        "  python fabriccicd.py all [ENV]    - Run all steps (default: DEV)\n"
    )

    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

    command = sys.argv[1].lower()
    env = sys.argv[2].upper() if len(sys.argv) > 2 else "DEV"

    if command == "create":
        headers = get_headers()
        create_workspaces(headers)

    elif command == "connect":
        headers = get_headers()
        connect_git(headers)

    elif command == "deploy":
        deploy(env)

    elif command == "all":
        headers = get_headers()
        create_workspaces(headers)
        if connect_git(headers):
            deploy(env)

    else:
        print(f"Unknown command: {command}\n")
        print(usage)
        sys.exit(1)


if __name__ == "__main__":
    main()
