# Fabric CICD
fabric-cicd is a Python library for Microsoft Fabric CI/CD automation. It supports code-first Continuous Integration/Continuous Deployment automations to seamlessly integrate Source Controlled workspaces into a deployment framework.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively
- Bootstrap and set up the development environment:
  - Ensure Python 3.9+ is installed
  - `pip install uv`
  - `uv sync --dev` -- NEVER CANCEL. Set timeout to 120+ seconds.
- Run tests:
  - `uv run pytest -v` -- NEVER CANCEL. Set timeout to 120+ seconds.
- Code formatting and linting:
  - `uv run ruff format` -- Apply formatting fixes.
  - `uv run ruff check` -- Check for linting issues.
  - `uv run ruff format --check` -- Check if formatting is needed.
- Documentation:
  - `uv run mkdocs build --clean` -- Build documentation.
  - `uv run mkdocs serve` -- starts local documentation server.

## Validation
- ALWAYS test library import functionality: `uv run python -c "from fabric_cicd import FabricWorkspace; print('Import successful')"`
- ALWAYS run through the complete test suite after making changes: `uv run pytest -v`
- ALWAYS run `uv run ruff format` and `uv run ruff check` before committing or the CI (.github/workflows/validate.yml) will fail
- The library requires Azure authentication (DefaultAzureCredential) for actual functionality - imports work without auth

## Project Structure
```
/
├── .github/workflows/    # CI/CD pipelines (test.yml, validate.yml, bump.yml)
├── docs/                # Documentation source files
├── sample/              # Example workspace structure and items
├── src/fabric_cicd/     # Main library source code
├── tests/               # Test files
├── pyproject.toml       # Project configuration and dependencies
├── ruff.toml           # Code formatting and linting configuration
├── mkdocs.yml          # Documentation configuration
├── activate.ps1        # PowerShell setup script (Windows only)
└── uv.lock            # Dependency lock file
```

## Common Tasks
Reference these validated outputs instead of running bash commands to save time:

### Import and Basic Usage
```python
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Initialize workspace (requires Azure auth)
workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    repository_directory="/path/to/workspace/items",
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
    environment="DEV"
)

# Deploy items
publish_all_items(workspace)

# Clean up orphaned items
unpublish_all_orphan_items(workspace)
```

### Test Categories
- **Unit Tests**: `tests/test_*.py` - Test individual components
- **Integration Tests**: Validate API interactions (mocked)
- **Parameter Tests**: Test parameterization and variable replacement
- **File Handling Tests**: Test various item type processing
- **Workspace Tests**: Test folder hierarchy and item management

### Key Dependencies
- `azure-identity` - Azure authentication
- `dpath` - JSON path manipulation  
- `pyyaml` - YAML parameter file processing
- `requests` - HTTP API calls
- `packaging` - Version handling

### Development Dependencies
- `uv` - Package manager and virtual environment
- `ruff` - Code formatting and linting
- `pytest` - Testing framework
- `mkdocs-material` - Documentation generation

### GitHub Actions Workflows
- **test.yml**: Runs `uv run pytest -v` on PR
- **validate.yml**: Runs `ruff format` and `ruff check` validation
- **bump.yml**: Handles version bumps (requires PR title format vX.X.X)

### Authentication Requirements
- Uses Azure DefaultAzureCredential by default
- Requires Azure CLI (`az login`) or Az.Accounts PowerShell module for local development
- Service principal authentication supported for CI/CD pipelines
- No authentication needed for basic library imports or testing

### Microsoft Fabric APIs
- The library primarily integrates with Microsoft Fabric Core APIs
- API documentation: https://learn.microsoft.com/en-us/rest/api/fabric/core/
- Common API operations include workspace management, item publishing, and artifact deployment

### Timing Expectations and Timeouts
- **CRITICAL**: NEVER CANCEL any build or test commands. Always use adequate timeouts:
  - uv sync: 120+ seconds
  - pytest: 120+ seconds  
  - All other commands: 60+ seconds

### Pull Request Requirements
- MUST be linked to an issue using "Fixes #123 - Short Description" format in PR title
- PR description should be a copilot generated summary
- MUST pass ruff formatting and linting checks
- MUST pass all tests
- Version bump PRs must follow specific format (title: vX.X.X, only change constants.py and changelog.md)

### Common Troubleshooting
- **Import errors**: Use `uv run python` instead of direct `python` to ensure virtual environment
- **Test failures**: Check if Azure credentials are interfering with mocked tests
- **Formatting issues**: Run `uv run ruff format` to auto-fix most issues
- **CI failures**: Usually due to missing `ruff format` or failing tests

### Repository Examples
See `sample/workspace/` for example Microsoft Fabric item structures and `docs/example/` for usage patterns in different CI/CD scenarios (Azure DevOps, GitHub Actions, local development).

### Key Files to Monitor
- `src/fabric_cicd/constants.py` - Version and configuration constants
- `src/fabric_cicd/fabric_workspace.py` - Main workspace management class
- `pyproject.toml` - Project dependencies and configuration
- `parameter.yml` - Environment-specific parameter template (in sample/)