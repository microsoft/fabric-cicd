# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for git diff utilities: get_changed_items() and validate_git_compare_ref()."""

from pathlib import Path
from unittest.mock import patch

import pytest

import fabric_cicd._common._git_diff_utils as git_utils
from fabric_cicd._common._exceptions import InputError
from fabric_cicd._common._validate_input import validate_git_compare_ref


# =============================================================================
# Tests for validate_git_compare_ref()
# =============================================================================


class TestValidateGitCompareRef:
    def test_accepts_common_valid_refs(self):
        assert validate_git_compare_ref("HEAD~1") == "HEAD~1"
        assert validate_git_compare_ref("main") == "main"
        assert validate_git_compare_ref("feature/my_branch") == "feature/my_branch"
        assert validate_git_compare_ref("release/v1.2.3") == "release/v1.2.3"

    def test_rejects_empty_string(self):
        with pytest.raises(InputError):
            validate_git_compare_ref("")

    def test_rejects_whitespace_only(self):
        with pytest.raises(InputError):
            validate_git_compare_ref("   ")

    def test_rejects_dash_prefixed(self):
        with pytest.raises(InputError):
            validate_git_compare_ref("-n")
        with pytest.raises(InputError):
            validate_git_compare_ref("--help")

    def test_rejects_invalid_characters(self):
        with pytest.raises(InputError):
            validate_git_compare_ref("ref;rm -rf /")


# =============================================================================
# Tests for get_changed_items()
# =============================================================================


class TestGetChangedItems:
    """Tests for the public get_changed_items() utility function."""

    def _make_git_diff_output(self, lines: list[str]) -> str:
        return "\n".join(lines)

    def test_returns_changed_items_from_git_diff(self, tmp_path):
        """Returns items detected as modified/added by git diff."""
        # Set up a fake item directory with a .platform file
        item_dir = tmp_path / "MyNotebook.Notebook"
        item_dir.mkdir()
        platform = item_dir / ".platform"
        platform.write_text(
            '{"metadata": {"type": "Notebook", "displayName": "MyNotebook"}}',
            encoding="utf-8",
        )
        changed_file = item_dir / "notebook.py"
        changed_file.write_text("print('hello')", encoding="utf-8")

        diff_output = self._make_git_diff_output(["M\tMyNotebook.Notebook/notebook.py"])

        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value.stdout = diff_output
            mock_run.return_value.returncode = 0

            result = git_utils.get_changed_items(tmp_path)

        assert result == ["MyNotebook.Notebook"]

    def test_returns_empty_list_when_no_changes(self, tmp_path):
        """Returns an empty list when git diff reports no changed files."""
        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value.stdout = ""
            mock_run.return_value.returncode = 0

            result = git_utils.get_changed_items(tmp_path)

        assert result == []

    def test_returns_empty_list_when_git_root_not_found(self, tmp_path):
        """Returns an empty list and logs a warning when no git root is found."""
        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with patch(git_root_patch, return_value=None):
            result = git_utils.get_changed_items(tmp_path)

        assert result == []

    def test_returns_empty_list_when_git_diff_fails(self, tmp_path):
        """Returns an empty list and logs a warning when git diff fails."""
        import subprocess

        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "git", stderr="bad ref")),
        ):
            result = git_utils.get_changed_items(tmp_path)

        assert result == []

    def test_uses_custom_git_compare_ref(self, tmp_path):
        """Passes the custom git_compare_ref to the underlying git command."""
        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value.stdout = ""
            mock_run.return_value.returncode = 0

            git_utils.get_changed_items(tmp_path, git_compare_ref="main")

        call_args = mock_run.call_args[0][0]
        assert "main" in call_args

    def test_excludes_files_outside_repository_directory(self, tmp_path):
        """Files changed outside the configured repository_directory are ignored."""
        outside_dir = tmp_path / "other_repo" / "SomeItem.Notebook"
        outside_dir.mkdir(parents=True)

        diff_output = self._make_git_diff_output(["M\tother_repo/SomeItem.Notebook/item.py"])

        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value.stdout = diff_output
            mock_run.return_value.returncode = 0

            # Use a subdirectory as the repository_directory so "other_repo" is out of scope
            repo_subdir = tmp_path / "my_workspace"
            repo_subdir.mkdir()
            result = git_utils.get_changed_items(repo_subdir)

        assert result == []
