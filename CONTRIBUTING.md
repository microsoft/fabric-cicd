# Contributing

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the
instructions provided by the bot. You will only need to do this once across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Ways to Contribute

We welcome several types of contributions:

- 🔧 **Bug fixes** - Fix issues and improve reliability
- ✨ **New features** - Add new commands or functionality
- 🆕 **New Items Support** - Onboard new Fabric item types
- 📝 **Documentation** - Improve guides, examples, and API docs
- 🧪 **Tests** - Add or improve test coverage
- 💬 **Help others** - Answer questions and provide support
- 💡 **Feature suggestions** - Propose new capabilities

## Prerequisites

Before you begin, ensure you have the following installed:

- [Python](https://www.python.org/downloads/) (see [Installation](https://microsoft.github.io/fabric-cicd/#installation) for version requirements)
- [Node.js and npm](https://nodejs.org/en/download/)
- [PowerShell](https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows) or [Az.Accounts PowerShell module](https://www.powershellgallery.com/packages/Az.Accounts/2.2.3)
- [Visual Studio Code (VS Code)](https://code.visualstudio.com/)

## Initial Configuration

1. **Fork the Repository on GitHub**:
    - Go to the repository [fabric-cicd](https://github.com/microsoft/fabric-cicd) on GitHub
    - In the top right corner, click on the **Fork** button
    - This will create a copy of the repository in your own GitHub account

1. **Clone Your Forked Repository**:
    - Once the fork is complete, go to your GitHub account and open the forked repository
    - Click on the **Code** button, and clone to VS Code

1. **Run activate.ps1**:
    - Open the Project in VS Code
    - Open PowerShell terminal
    - Run `activate.ps1` which will install `uv`, and `ruff` if not already found. And set up the default environment leveraging `uv sync`
        ```powershell
        .\activate.ps1
        ```
        _Note that this is technically optional and is designed to work with PowerShell. You can execute these steps manually as well, this is merely a helper_
        _For Linux, run `activate.sh` instead_

1. **Select Python Interpreter**:
    - Open the Command Palette (`Ctrl+Shift+P`) and select `Python: Select Interpreter`
    - Choose the interpreter from the `.venv` directory

1. **Ensure All VS Code Extensions Are Installed**:
    - Open the Command Palette (`Ctrl+Shift+P`) and select `Extensions: Show Recommended Extensions`
    - Install all extensions recommended for the workspace

## Development

### Managing Dependencies

- All dependencies in this project are managed by `uv` which will resolve all dependencies and lock the versions to speed up virtual environment creation
- For additions, run:
    ```sh
    uv add <package-name>
    ```
- For removals, run:
    ```sh
    uv remove <package-name>
    ```

### Code Formatting & Linting

- The python code within this project is maintained by `ruff`
- If you install the recommended extensions, `ruff` will auto format on save of any file
- Before being able to merge a PR, `ruff` is ran in a GitHub Action to ensure the files are properly formatted and maintained
- To force linting, run the following
    ```sh
    uv run ruff format
    uv run ruff check
    ```

## Contribution process

To avoid cases where submitted PRs are rejected, please follow the following steps:

- To report a new issue, follow [Create an issue](#creating-an-issue)
- To work on existing issue, follow [Find an issue to work on](#finding-an-issue-to-work-on)
- To contribute code, follow [Pull request process](#pull-request-process)

### Creating an issue

Before reporting a new bug or suggesting a feature, please search the [GitHub Issues page](https://github.com/microsoft/fabric-cicd/issues) to check if one already exists.

All reported bugs or feature suggestions must start with creating an issue in the GitHub Issues pane. Please add as much information as possible to help us with triage and understanding. Once the issue is triaged, labels will be added to indicate its status (e.g., "need more info", "help wanted").

When creating an issue please select the relevant template, e.g., bug, new feature, general question, etc. and provide all required input:

- [Bug Report](https://github.com/microsoft/fabric-cicd/issues/new?template=1-bug.yml)
- [Feature Request](https://github.com/microsoft/fabric-cicd/issues/new?template=2-feature.yml)
- [Documentation](https://github.com/microsoft/fabric-cicd/issues/new?template=3-documentation.yml)
- [Question](https://github.com/microsoft/fabric-cicd/issues/new?template=4-question.yml)
- [Copilot](https://github.com/microsoft/fabric-cicd/issues/new?template=5-copilot.yml)

We aim to respond to new issues promptly, but response times may vary depending on workload and priority.

### Finding an issue to work on

#### For Beginners

If you're new to contributing, look for issues with these labels:

- **`good-first-issue`** - Beginner-friendly tasks that are well-scoped and documented
- **`help wanted`** - Issues where community contributions are especially welcome
- **`documentation`** - Improve docs, examples, or help text (great for first contributions)

#### Getting Started Tips

1. **Start small** - Look for typo fixes, documentation improvements, or simple bug fixes
2. **Read existing code** - Familiarize yourself with the codebase by exploring similar commands
3. **Ask questions** - Comment on issues to clarify requirements or get guidance
4. **Test locally** - Always test your changes thoroughly before submitting

#### Before You Code

All PRs must be linked with a "help wanted" issue. To avoid rework after investing effort:

1. **Comment on the issue** - Express interest and describe your planned approach
2. **Wait for acknowledgment** - Get team confirmation before starting significant work
3. **Ask for clarification** - Don't hesitate to ask questions about requirements

Please review [engineering guidelines](https://github.com/microsoft/fabric-cicd/wiki) for coding guidelines and common flows to help you with your task.

### Pull request process

**All pull requests must be linked to an approved issue,** see [PR Title Format](#pr-title-format). This ensures proper tracking and context for changes. Before creating a pull request:

1. **Create or identify an existing issue** that describes the problem, feature request, or change you're addressing
2. **Comment on the issue** to express interest and get team acknowledgment before starting work

#### PR Title Format

Your PR title MUST follow this exact format: `"Fixes #123 - Short Description"` where #123 is the issue number.

- Use "Fixes" for bug fixes, "Closes" for features, "Resolves" for other changes
- Example: "Fixes #520 - Add Python version requirements to documentation"
- Version bump PRs are an exception: title must be "vX.X.X" format only
- GitHub Actions will automatically check that your PR is linked to a valid issue and will fail if no valid reference is found

#### Before Submitting PR

Verify that:

- The PR is focused on the related task
- Tests coverage is kept and all tests pass
- Your code is aligned with the code conventions of this project

#### Review Process

- Use a descriptive title and provide a clear summary of your changes
- Address and resolve all review comments before merge
- PRs will be labeled as "need author feedback" when there are comments to resolve
- Approved PRs will be merged by the fabric-cicd team

### Documenting Changes with Changie

All pull requests must include proper change documentation using [changie](https://changie.dev), which is pre-installed in the development container. This ensures that release notes are automatically generated and changes are properly tracked.

#### Requirements

**Every PR must include at least one change entry** created using `changie new`. You may add multiple entries if your PR introduces multiple distinct changes.

#### How to Add Change Entries

1. **From the Terminal, run `changie new` command**:

    ```bash
    changie new
    ```

2. **Select the appropriate change type** from the available options:
    - **⚠️ Breaking Change** - For changes that break backward compatibility
    - **🆕 New Items Support** - For adding support for new Fabric item types
    - **✨ New Functionality** - For new features, commands, or capabilities
    - **🔧 Bug Fix** - For fixing existing issues or incorrect behavior
    - **⚡ Additional Optimizations** - For performance improvements or optimizations
    - **📝 Documentation Update** - For documentation improvements or updates

3. **Provide a clear description** of your change:
    - Write in present tense (e.g., "Add support for..." not "Added support for...")
    - Be specific and user-focused
    - Include the affected command or feature if applicable
    - Keep it concise but informative

#### Examples of Good Change Descriptions

- `Add new 'fab describe capacity' command for viewing capacity details`
- `Fix authentication timeout issue in interactive mode`
- `Update workspace examples with new folder hierarchy patterns`
- `Optimize API response caching to reduce network calls`

#### Guidelines

- **One logical change per entry**: If your PR fixes a bug and adds a feature, create two separate entries
- **User-facing perspective**: Describe what users will experience, not internal implementation details
- **Clear and actionable**: Users should understand what changed and how it affects them
- **Consistent formatting**: Follow the examples and existing patterns in the changelog

The change entries will be automatically included in the release notes when a new version is published. This process ensures that all improvements, fixes, and new features are properly communicated to users.

## Resources to help you get started

Here are some resources to help you get started:

- A good place to start learning about fabric-cicd is the [fabric-cicd documentation](https://microsoft.github.io/fabric-cicd/)
- If you want to contribute code, please check more details about coding guidelines, major code flows and code building block in [Engineering guidelines](https://github.com/microsoft/fabric-cicd/wiki)

## Engineering guidelines

For detailed engineering guidelines please refer to our [Wiki pages](https://github.com/microsoft/fabric-cicd/wiki).

The Wiki contains essential information and requirements for contributors, including: Code Style and Standards, Architecture Overview, Testing and more.

Before contributing code, please review these guidelines to ensure your contributions align with the project's standards and practices.

## Areas with Restricted Contributions

Some areas require special consideration:

- **Core infrastructure** - Major architectural changes require team discussion, including within `FabricEndpoint` and `FabricWorkspace` classes
- **Parameterization framework** - Changes require team discussion due to complex validation and parameter replacement logic

## Need Help?

### Getting Support

- **[GitHub Issues](https://github.com/microsoft/fabric-cicd/issues)** - Report specific problems
- **[Documentation](https://microsoft.github.io/fabric-cicd/)** - Check comprehensive guides

### Communication Guidelines

- **Be patient** - Maintainers balance multiple responsibilities
- **Be respectful** - Follow the code of conduct
- **Be specific** - Provide clear, detailed information
- **Be collaborative** - Work together to improve the project

Thank you for contributing to Microsoft fabric-cicd! Your contributions help make this tool better for the entire Fabric community.
