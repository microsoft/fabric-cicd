# Test Fixtures

## Capturing HTTP Trace for Mock REST Server for new resource types

Update `item_types_to_deploy` in the script, then run:

```bash
uv run python devtools/debug_trace_publish_all.py
```

New routes are automatically merged into `http_trace.csv`, skipping duplicates.
