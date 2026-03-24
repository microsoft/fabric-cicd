# Fabric CICD

fabric-cicd is a Python library for Microsoft Fabric CI/CD automation. It supports code-first Continuous Integration/Continuous Deployment automations to integrate Source Controlled workspaces into a deployment framework.

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Quick Command Reference

**Prerequisites**: Requires Python 3.9+

| Task         | Command                                                                                  | Timeout |
| ------------ | ---------------------------------------------------------------------------------------- | ------- |
| Setup        | `pip install uv && uv sync --dev` (NEVER CANCEL)                                         | 120+s   |
| Test         | `uv run pytest -v` (NEVER CANCEL)                                                        | 120+s   |
| Import check | `uv run python -c "from fabric_cicd import FabricWorkspace; print('Import successful')"` | 30s     |
| Format       | `uv run ruff format` (Fix formatting issues)                                             | 60s     |
| Lint check   | `uv run ruff check` (Check for linting issues)                                           | 60s     |
| Format check | `uv run ruff format --check` (Verify formatting is correct)                              | 60s     |
| Docs build   | `uv run mkdocs build --clean` (Build documentation)                                      | 60s     |
| Docs serve   | `uv run mkdocs serve` (Start local documentation server)                                 | 60s     |

**Mandatory Validation (ALWAYS):**

1. Import check → 2. Run tests → 3. Format code → 4. Check linting → 5. Commit

**Critical**: NEVER cancel build/test commands. CI (`.github/workflows/validate.yml`) will fail if validation workflow incomplete.

## Authentication

Must provide explicit `token_credential` parameter to `FabricWorkspace`.

**Methods:**

- **Local development**: `AzureCliCredential()` or `AzurePowerShellCredential()`
- **CI/CD pipelines**: `ClientSecretCredential()` with service principal
- **Testing/imports**: No authentication needed

**Example:**

```python
from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace

token_credential = AzureCliCredential()
workspace = FabricWorkspace(
    workspace_id="your-id",
    token_credential=token_credential
)
```

## Basic Usage

### Programmatic API

```python
from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Initialize workspace
token_credential = AzureCliCredential()
workspace = FabricWorkspace(
    workspace_id="your-workspace-id",
    environment="DEV",
    repository_directory="/path/to/workspace/items",
    item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
    token_credential=token_credential
)

# Deploy items
publish_all_items(workspace)

# Clean up orphaned items
unpublish_all_orphan_items(workspace)
```

### Config-Based Deployment

Alternative: `deploy_with_config()` centralizes deployment settings in YAML.

```python
from fabric_cicd import deploy_with_config
result = deploy_with_config(config_file_path="config.yml", environment="dev")
```

**Implementation files:**

- Entry point: `deploy_with_config()` in `src/fabric_cicd/publish.py`
- Config utilities: `src/fabric_cicd/_common/_config_utils.py` (loading, extraction)
- Config validation: `src/fabric_cicd/_common/_config_validator.py`
- Documentation: `docs/how_to/config_deployment.md`
- Tests: `tests/test_deploy_with_config.py`, `tests/test_config_validator.py`

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

## Development Guidelines

### Core Concepts

- **Publisher Classes**: Handle deployment logic for each Fabric item type in `src/fabric_cicd/_items/`
- **Serial Publishing**: Items deploy in dependency order via `SERIAL_ITEM_PUBLISH_ORDER`
- **Parameterization**: YAML-based environment-specific value replacement

### Common Development Patterns

- **Adding constants**: Add to `ItemType` enum + `SERIAL_ITEM_PUBLISH_ORDER`
- **Adding publisher**: Extend `ItemPublisher` + register in `_base_publisher.py` factory

### Testing Guidelines

**Always add/update tests for:**

- New functionality or features
- Bug fixes that change behavior
- Core logic changes in any module
- Publisher classes and deployment logic
- Configuration and validation logic
- API integrations and external calls

**Testing approach**: Mock all external dependencies, use `requests_mock` for Azure APIs, `tmpdir` for file operations. Focus on testing business logic, error handling, and integration points.

### Files to Avoid Modifying

- `coverage_report/`, `site/`, `htmlcov/` - Auto-generated
- `uv.lock` - Managed by uv
- `.github/workflows/` - Affects CI validation

### Dependencies & Testing

**Runtime:** `azure-identity`, `dpath`, `pyyaml`, `requests`, `packaging`  
**Development:** `uv`, `ruff`, `pytest`, `mkdocs-material`

**Test Types:** Unit (`tests/test_*.py`), Integration (mocked APIs), Parameter/File Handling, Workspace management

**GitHub Actions:** `test.yml` (PR tests), `validate.yml` (formatting/linting), `bump.yml` (version bumps - vX.X.X format)

**Microsoft Fabric APIs:** https://learn.microsoft.com/en-us/rest/api/fabric/

## Pull Request Requirements

**Title format:** "Fixes #123 - Short Description" where #123 is the issue number

- Use "Fixes" for bug fixes, "Closes" for features, "Resolves" for other changes
- Example: "Fixes #520 - Add Python version requirements to documentation"
- Exception: Version bump PRs use "vX.X.X" format only

**Requirements:**

- PR description should be copilot generated summary
- Pass ruff formatting and linting checks
- Pass all tests
- All PRs must be linked to valid GitHub issue

## Agent Troubleshooting

**Common Failures:**

- **Import errors**: Use `uv run python` prefix to ensure virtual environment
- **Test pollution**: Azure credentials interfering - ensure proper mocking
- **Setup failures**: Run `uv sync --dev` if modules missing
- **CI failures**: Missing format/lint step in validation workflow

**Authentication Strategy for Agents:**

1. For testing/imports: No auth needed
2. For publish operations: Use `AzureCliCredential()` (If `CredentialUnavailableError` occurs, user needs to run `az login` first)
3. Context: Import check works without auth, but publish operations require credentials

## Key Files to Monitor

**Core System Files:**

- `src/fabric_cicd/constants.py` - Version and configuration constants
- `src/fabric_cicd/fabric_workspace.py` - Main workspace management class
- `src/fabric_cicd/publish.py` - Main deployment entry points
- `src/fabric_cicd/_items/` - Publisher classes for all item types

**Configuration Files:**

- `pyproject.toml` - Project dependencies and configuration
- `sample/workspace/parameter.yml` - Environment-specific parameter template

**Project Structure:**

- `sample/workspace/` - Example Microsoft Fabric item structures
