# Fabric CICD
fabric-cicd is a Python library for Microsoft Fabric CI/CD automation. It supports code-first Continuous Integration/Continuous Deployment automations to seamlessly integrate Source Controlled workspaces into a deployment framework.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively
- Bootstrap and set up the development environment:
  - Ensure Python 3.9-3.13 is installed
  - `pip install uv`
  - `uv sync --dev` -- takes 30-60 seconds. NEVER CANCEL. Set timeout to 120+ seconds.
- Run tests:
  - `uv run pytest -v` -- takes 64 seconds (139 tests). NEVER CANCEL. Set timeout to 120+ seconds.
- Code formatting and linting:
  - `uv run ruff format` -- takes <1 second. Apply formatting fixes.
  - `uv run ruff check` -- takes <1 second. Check for linting issues.
  - `uv run ruff format --check` -- takes <1 second. Check if formatting is needed.
- Documentation:
  - `uv run mkdocs build --clean` -- takes 1-2 seconds. Build documentation.
  - `uv run mkdocs serve` -- starts local documentation server.

## Validation
- ALWAYS test library import functionality: `uv run python -c "from fabric_cicd import FabricWorkspace; print('Import successful')"`
- ALWAYS run through the complete test suite after making changes: `uv run pytest -v`
- Test execution typically takes ~64 seconds for 139 tests - this is normal
- ALWAYS run `uv run ruff format` and `uv run ruff check` before committing or the CI (.github/workflows/validate.yml) will fail
- The library requires Azure authentication (DefaultAzureCredential) for actual functionality - imports work without auth

## Project Structure
```
/
├── .github/workflows/    # CI/CD pipelines (test.yml, validate.yml, bump.yml)
├── docs/                # Documentation source files
├── sample/              # Example workspace structure and items
├── src/fabric_cicd/     # Main library source code
├── tests/               # Test files (139 tests total)
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
- `azure-identity>=1.19.0` - Azure authentication
- `dpath>=2.2.0` - JSON path manipulation  
- `pyyaml>=6.0.2` - YAML parameter file processing
- `requests>=2.32.3` - HTTP API calls
- `packaging>=24.2` - Version handling

### Development Dependencies
- `uv` - Package manager and virtual environment
- `ruff>=0.9.5` - Code formatting and linting
- `pytest>=8.3.4` - Testing framework
- `mkdocs-material>=9.6.5` - Documentation generation

### GitHub Actions Workflows
- **test.yml**: Runs `uv run pytest -v` on PR
- **validate.yml**: Runs `ruff format` and `ruff check` validation
- **bump.yml**: Handles version bumps (requires PR title format vX.X.X)

### Expected Item Types
The library supports these Microsoft Fabric item types:
- Notebook, DataPipeline, Environment, Report, SemanticModel
- Warehouse, SQLDatabase, Lakehouse, Eventhouse, KQLDashboard
- Dataflow, CopyJob, GraphQLApi, Reflex, Eventstream
- KQLQueryset, MirroredDatabase, VariableLibrary

### Authentication Requirements
- Uses Azure DefaultAzureCredential by default
- Requires Azure CLI (`az login`) or Az.Accounts PowerShell module for local development
- Service principal authentication supported for CI/CD pipelines
- No authentication needed for basic library imports or testing

### Timing Expectations and Timeouts
- **Environment setup**: 30-60 seconds (uv sync)
- **Test execution**: 64 seconds for full suite (139 tests)
- **Linting/formatting**: <1 second each
- **Documentation build**: 1-2 seconds
- **Library import validation**: <1 second
- **CRITICAL**: NEVER CANCEL any build or test commands. Always use adequate timeouts:
  - uv sync: 120+ seconds
  - pytest: 120+ seconds  
  - All other commands: 60+ seconds

### Pull Request Requirements
- MUST be linked to an issue using "Fixes #123", "Closes #456", or "Resolves #789" in PR title/description
- MUST pass ruff formatting and linting checks
- MUST pass all 139 tests
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