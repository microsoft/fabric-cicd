# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Utility functions for detecting Fabric items changed via git diff."""

import json
import logging
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _find_platform_item(file_path: Path, repo_root: Path) -> Optional[tuple[str, str]]:
    """
    Walk up from file_path towards repo_root looking for a .platform file.

    The .platform file marks the boundary of a Fabric item directory.
    Its JSON content contains ``metadata.type`` (item type) and
    ``metadata.displayName`` (item name).

    Returns:
        A ``(item_name, item_type)`` tuple, or ``None`` if not found.
    """
    current = file_path.parent
    while True:
        platform_file = current / ".platform"
        if platform_file.exists():
            try:
                data = json.loads(platform_file.read_text(encoding="utf-8"))
                metadata = data.get("metadata", {})
                item_type = metadata.get("type")
                item_name = metadata.get("displayName") or current.name
                if item_type:
                    return item_name, item_type
            except Exception as exc:
                logger.debug(f"Could not parse .platform file at '{platform_file}': {exc}")
        # Stop if we have reached the repository root or the filesystem root
        if current == repo_root or current == current.parent:
            break
        current = current.parent
    return None


def _resolve_git_diff_path(
    file_path_str: str,
    git_root: Path,
    repository_directory: Path,
) -> Optional[Path]:
    """
    Resolve and validate a file path from git diff output.

    Follows the same resolve → boundary-check → reject contract as
    ``_resolve_file_path`` in ``_parameter/_utils.py``, adapted for
    paths that are relative to a git root with containment checked
    against a (potentially different) repository subdirectory.

    Args:
        file_path_str: Relative path string from git diff output.
        git_root: Resolved absolute path of the git repository root.
        repository_directory: Resolved absolute path of the configured
            repository directory (may be a subdirectory of git_root).

    Returns:
        Resolved absolute Path if valid and within boundary, None otherwise.
    """
    raw_path = Path(file_path_str)

    # Reject absolute paths — git diff should only produce relative paths
    if raw_path.is_absolute():
        logger.debug(f"get_changed_items: skipping absolute path '{file_path_str}'")
        return None

    # Reject traversal sequences before resolution (mirrors _validate_wildcard_syntax)
    if ".." in raw_path.parts:
        logger.debug(f"get_changed_items: skipping path with traversal '{file_path_str}'")
        return None

    # Reject null bytes
    if "\x00" in file_path_str:
        logger.debug("get_changed_items: skipping path with null bytes")
        return None

    # Step 1: Resolve relative to git root (analogous to _resolve_file_path Step 1)
    resolved_path = (git_root / file_path_str).resolve()

    # Step 2: Boundary check against repository_directory (analogous to _resolve_file_path Step 2)
    try:
        resolved_path.relative_to(repository_directory)
    except ValueError:
        return None

    # Note: No Step 3 (existence check) — deleted files won't exist on disk
    return resolved_path


def get_changed_items(
    repository_directory: Path,
    git_compare_ref: str = "HEAD~1",
) -> list[str]:
    """
    Return the list of Fabric items that were added, modified, or renamed relative to ``git_compare_ref``.

    The returned list is in ``"item_name.item_type"`` format and can be passed directly
    to the ``items_to_include`` parameter of :func:`publish_all_items` to deploy only
    what has changed since the last commit.

    Args:
        repository_directory: Path to the local git repository directory
            (e.g. ``FabricWorkspace.repository_directory``).
        git_compare_ref: Git ref to compare against. Defaults to ``"HEAD~1"``.

    Returns:
        List of strings in ``"item_name.item_type"`` format. Returns an empty list when
        no changes are detected, the git root cannot be found, or git is unavailable.

    Examples:
        Deploy only changed items
        >>> from azure.identity import AzureCliCredential
        >>> from fabric_cicd import FabricWorkspace, publish_all_items, get_changed_items
        >>> workspace = FabricWorkspace(
        ...     workspace_id="your-workspace-id",
        ...     repository_directory="/path/to/repo",
        ...     item_type_in_scope=["Notebook", "DataPipeline"],
        ...     token_credential=AzureCliCredential()
        ... )
        >>> changed = get_changed_items(workspace.repository_directory)
        >>> if changed:
        ...     publish_all_items(workspace, items_to_include=changed)

        With a custom git ref
        >>> changed = get_changed_items(workspace.repository_directory, git_compare_ref="main")
        >>> if changed:
        ...     publish_all_items(workspace, items_to_include=changed)
    """
    changed, _ = _resolve_changed_items(Path(repository_directory), git_compare_ref)
    return changed


def _resolve_changed_items(
    repository_directory: Path,
    git_compare_ref: str,
) -> tuple[list[str], list[str]]:
    """
    Use ``git diff --name-status`` to detect Fabric items that changed or were
    deleted relative to *git_compare_ref*.

    Args:
        repository_directory: Absolute path to the local repository directory
            (as stored on ``FabricWorkspace.repository_directory``).
        git_compare_ref: Git ref to diff against (e.g. ``"HEAD~1"``).

    Returns:
        A two-element tuple ``(changed_items, deleted_items)`` where each
        element is a list of strings in ``"item_name.item_type"`` format.
        Both lists are empty when the git root cannot be found or git fails.
    """
    from fabric_cicd._common._config_validator import _find_git_root
    from fabric_cicd._common._validate_input import validate_git_compare_ref

    validate_git_compare_ref(git_compare_ref)

    git_root = _find_git_root(repository_directory)
    if git_root is None:
        logger.warning("get_changed_items: could not locate a git repository root — returning empty list.")
        return [], []

    try:
        result = subprocess.run(
            ["git", "diff", "--name-status", git_compare_ref],
            cwd=str(git_root),
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
    except subprocess.CalledProcessError as exc:
        logger.warning(f"get_changed_items: 'git diff' failed ({exc.stderr.strip()}) — returning empty list.")
        return [], []
    except subprocess.TimeoutExpired:
        logger.warning("get_changed_items: 'git diff' timed out — returning empty list.")
        return [], []

    changed_items: set[str] = set()
    deleted_items: set[str] = set()

    git_root_resolved = git_root.resolve()
    repo_dir_resolved = repository_directory.resolve()

    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        status = parts[0].strip()

        # Renames produce three tab-separated fields: R<score>\told\tnew
        if status.startswith("R") and len(parts) >= 3:
            file_path_str = parts[2]
        elif len(parts) >= 2:
            file_path_str = parts[1]
        else:
            continue

        abs_path = _resolve_git_diff_path(file_path_str, git_root_resolved, repo_dir_resolved)
        if abs_path is None:
            continue

        if status == "D":
            if abs_path.name == ".platform":
                try:
                    show_result = subprocess.run(
                        ["git", "show", f"{git_compare_ref}:{file_path_str}"],
                        cwd=str(git_root_resolved),
                        capture_output=True,
                        text=True,
                        check=True,
                        timeout=30,
                    )
                    data = json.loads(show_result.stdout)
                    metadata = data.get("metadata", {})
                    item_type = metadata.get("type")
                    item_name = metadata.get("displayName") or abs_path.parent.name
                    if item_type and item_name:
                        deleted_items.add(f"{item_name}.{item_type}")
                except Exception as exc:
                    logger.debug(f"get_changed_items: could not read deleted .platform '{file_path_str}': {exc}")
        else:
            item_info = _find_platform_item(abs_path, repo_dir_resolved)
            if item_info:
                changed_items.add(f"{item_info[0]}.{item_info[1]}")

    return list(changed_items), list(deleted_items)
