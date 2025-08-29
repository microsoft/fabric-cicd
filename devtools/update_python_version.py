#!/usr/bin/env python3
"""
Script to update Python version requirements in documentation files.
This script reads the requires-python field from pyproject.toml and updates
all documentation files to reflect the current Python version requirements.
"""

import re
import sys
from pathlib import Path


def get_python_version_requirements():
    """Parse pyproject.toml to extract Python version requirements."""
    script_dir = Path(__file__).resolve().parent
    # Go up to find the root directory
    root_directory = script_dir.parent
    pyproject_path = root_directory / "pyproject.toml"

    if not pyproject_path.exists():
        print(f"Warning: {pyproject_path} not found, using fallback version")
        return "3.9 to 3.12"  # fallback

    with pyproject_path.open() as f:
        content = f.read()

    # Find the requires-python line
    match = re.search(r'requires-python\s*=\s*"([^"]+)"', content)
    if not match:
        print("Warning: requires-python not found in pyproject.toml, using fallback version")
        return "3.9 to 3.12"  # fallback

    requires_python = match.group(1)
    print(f"Found requires-python: {requires_python}")

    # Parse version constraints like ">=3.9,<3.13"
    min_version_match = re.search(r">=(\d+\.\d+)", requires_python)
    max_version_match = re.search(r"<(\d+\.\d+)", requires_python)

    if min_version_match and max_version_match:
        min_version = min_version_match.group(1)
        max_version_str = max_version_match.group(1)
        # Convert max version to the last supported version
        # e.g., "<3.13" means "up to 3.12"
        max_parts = max_version_str.split(".")
        max_major = int(max_parts[0])
        max_minor = int(max_parts[1])
        actual_max = f"{max_major}.{max_minor - 1}"
        return f"{min_version} to {actual_max}"

    print("Warning: Could not parse version constraints, using fallback version")
    return "3.9 to 3.12"  # fallback


def update_file(file_path, python_version):
    """Update Python version requirements in a single file."""
    if not file_path.exists():
        print(f"Warning: {file_path} not found, skipping")
        return False

    with file_path.open() as f:
        content = f.read()

    original_content = content

    # Replace patterns in documentation
    patterns = [
        (r"\*\*Requirements\*\*:\s*Python\s+[\d\.]+ to [\d\.]+", f"**Requirements**: Python {python_version}"),
        (r"\(version\s+[\d\.]+ to [\d\.]+\)", f"(version {python_version})"),
    ]

    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)

    if content != original_content:
        with file_path.open("w") as f:
            f.write(content)
        print(f"Updated {file_path}")
        return True
    print(f"No changes needed in {file_path}")
    return False


def main():
    """Main function to update all documentation files."""
    script_dir = Path(__file__).resolve().parent
    root_directory = script_dir.parent

    python_version = get_python_version_requirements()
    print(f"Updating documentation with Python version: {python_version}")

    # Files to update
    files_to_update = [
        root_directory / "README.md",
        root_directory / "docs" / "index.md",
        root_directory / "docs" / "contribution.md",
    ]

    updated_files = []
    for file_path in files_to_update:
        if update_file(file_path, python_version):
            updated_files.append(file_path)

    if updated_files:
        print(f"\nSuccessfully updated {len(updated_files)} files:")
        for file_path in updated_files:
            print(f"  - {file_path.relative_to(root_directory)}")
    else:
        print("\nNo files were updated (all already had correct version)")


if __name__ == "__main__":
    main()
