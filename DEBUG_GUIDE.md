# Debugging fabric-cicd

Quick guide to debug the `publish_all_items()` workflow.

## Setup

```bash
# Install dependencies
uv sync --dev

# Authenticate
az login
```

### Configuration

Edit lines 16-17 in [debug_publish_all.py](devtools/debug_publish_all.py):

```python
workspace_id = "your-workspace-id"  # Your target workspace
environment_key = "PPE"  # Must match sample/workspace/parameter.yml
```

## Debugging

### Quick Start

1. Open [devtools/debug_publish_all.py](devtools/debug_publish_all.py)
2. Set breakpoint at line 101 (click left gutter)
3. Press **F5** → Select "Debug: Publish All Items"
