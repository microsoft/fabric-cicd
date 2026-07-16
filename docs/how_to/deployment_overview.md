# Deployment Overview

## Git Flow

The flow pictured below is the hero scenario for this library and is the recommendation if you're just starting out.

- `Deployed` branches are not connected to workspaces via [Git Sync](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=azure-devops%2CAzure%2Ccommit-to-git#connect-a-workspace-to-a-git-repo)
- `Feature` branches are connected to workspaces via [Git Sync](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-get-started?tabs=azure-devops%2CAzure%2Ccommit-to-git#connect-a-workspace-to-a-git-repo)
- `Deployed` workspaces are only updated through script-based deployments, such as through the fabric-cicd library
- `Feature` branches are created from the default branch, merged back into the default `Deployed` branch, and cherry-picked into the upper `Deployed` branches

![Git Flow](../config/assets/git_flow.png)

## Deployment Philosophy

fabric-cicd applies a **full deployment** on every run — it does not inspect commit history or compute diffs. This ensures the target workspace always reflects the repository and converges to the desired state regardless of what happened in previous runs, preventing environment drift.

Once items are committed to source control via [Fabric Source Control](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration), fabric-cicd reads the item definition files from the repository directory and passes the content as a payload to the [Fabric REST APIs](https://learn.microsoft.com/en-us/rest/api/fabric/), which create or update the corresponding items in the target workspace. Every item defined in `item_type_in_scope` is created or updated on each run. Use `unpublish_all_orphan_items()` to remove workspace items that are no longer in the repository.

> **Note:** While full deployment is the default and recommended approach, [selective deployment](optional_feature.md#selective-deployment-features) and [git-based change detection](optional_feature.md#git-based-change-detection) are available as experimental features. Selective deployment is not recommended due to the risk of broken deployments caused by unresolved item dependencies.
