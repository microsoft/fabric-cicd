# Debugging fabric-cicd

Quick guide to debug the public API - `publish_all_items()` workflow.

## Setup

```bash
# Install dependencies
uv sync --dev

# Authenticate
az login
```
## Debugging

1. Open [devtools/debug_publish_all.py](devtools/debug_publish_all.py)
1. Set breakpoint
1. Update `.vscode/launch.json` with your workspace id in `FABRIC_WORKSPACE_ID`
1. Press **F5** → Select "Debug: Publish All Items"
