# Test Fixtures

## `mock_fabric_server`: A mock Fabric REST API

This is a mock REST API Server that mimics `https://api.powerbi.com`.
The idea is, to exercise the public facing `fabric_cicd` API E2E rapidly.
The mock server loads an `http_trace.json` file to dictate it's behavior.

### Capturing HTTP Trace for new resource types

Suppose you need to add payloads for a new resource type.

Update `item_types_to_deploy` in the script with the item you want to capture HTTP traffic for, then run:

```bash
export FABRIC_WORKSPACE_ID="8847d306-2a0c-4dc6-9fda-125b7d4fb966"
uv run python devtools/debug_trace_publish_all.py
```

New routes are automatically merged into `http_trace.json`, skipping duplicates.

You can validate the integration test works with the mock server with:

```bash
uv run pytest -v -s --log-cli-level=INFO tests/test_integration_publish.py::test_publish_all_items_integration
```
