# Test Fixtures

## `mock_fabric_server`: A mock Fabric REST API

This is a mock REST API Server that mimics `https://api.powerbi.com`.
The idea is, to exercise the public facing `fabric_cicd` API E2E rapidly.
The mock server loads an `http_trace.json` file to dictate the behavior.

### What is this?

The 4 steps outlined in the image below are as follows:

1. Add new workloads into the codebase
2. Capture REST calls from Fabric using `debug_trace_deployment.py`
3. Move `http_trace.json.gz` into fixture
4. Enjoy rapid test coverage!


![Test Harness](.imgs/test-harness.png)

### Capturing HTTP Trace for new fabric item types

Suppose you need to add payloads for a new fabric item type.

The following script creates an HTTP snapshot that is stored in `http_trace.json.gz`, which is moved into `fabric-cicd/tests/fixtures`

Update `item_types_to_deploy` in the script with the item you want to capture HTTP traffic for, then run:

```bash
export FABRIC_WORKSPACE_ID="your-fabric-workspace-guid"
uv run python devtools/debug_trace_deployment.py
cp -f http_trace.json.gz tests/fixtures/http_trace.json.gz
```

You can validate the integration test works with the mock server with:

```bash
uv run pytest -v -s --log-cli-level=INFO tests/test_integration_publish.py::test_publish_all_items_integration
```

### Important Notes

* The `http_trace.json` must be generated in one shot, i.e. the Mock Server is not guaranteed to incrementally process new lines added to `http_trace.json`.
  What that means is - you should capture as many items as possible in `debug_trace_deployment.py`, and use that payload in the tests.
