# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for git diff utilities: get_changed_items() and validate_git_compare_ref()."""

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

    def test_rejects_shell_metacharacters(self):
        """Prevent shell injection via backticks, pipes, dollar signs, etc."""
        for ref in ["$(whoami)", "`id`", "ref|cat /etc/passwd", "ref&echo bad", "ref\nnewline"]:
            with pytest.raises(InputError):
                validate_git_compare_ref(ref)

    def test_rejects_non_string_input(self):
        """Non-string types must be rejected."""
        with pytest.raises(InputError):
            validate_git_compare_ref(123)
        with pytest.raises(InputError):
            validate_git_compare_ref(None)

    def test_accepts_advanced_git_ref_syntax(self):
        """Valid git ref syntax including caret, tilde, and reflog notation."""
        assert validate_git_compare_ref("HEAD^") == "HEAD^"
        assert validate_git_compare_ref("HEAD~3") == "HEAD~3"
        assert validate_git_compare_ref("main@{1}") == "main@{1}"
        assert validate_git_compare_ref("origin/main") == "origin/main"
        assert validate_git_compare_ref("v2.0.0") == "v2.0.0"
        assert validate_git_compare_ref("abc123def") == "abc123def"


# =============================================================================
# Tests for _resolve_git_diff_path()
# =============================================================================


class TestResolveGitDiffPath:
    """Tests for path validation/resolution from git diff output."""

    def test_rejects_absolute_paths(self, tmp_path):
        result = git_utils._resolve_git_diff_path("/etc/passwd", tmp_path, tmp_path)
        assert result is None

    def test_rejects_path_traversal(self, tmp_path):
        result = git_utils._resolve_git_diff_path("../../../etc/passwd", tmp_path, tmp_path)
        assert result is None

    def test_rejects_null_bytes(self, tmp_path):
        result = git_utils._resolve_git_diff_path("file\x00.txt", tmp_path, tmp_path)
        assert result is None

    def test_accepts_valid_relative_path(self, tmp_path):
        sub = tmp_path / "MyItem"
        sub.mkdir()
        result = git_utils._resolve_git_diff_path("MyItem/file.py", tmp_path, tmp_path)
        assert result is not None
        assert result.name == "file.py"

    def test_rejects_path_outside_repo_directory(self, tmp_path):
        """A file under git root but outside the repo subdirectory is rejected."""
        repo_sub = tmp_path / "workspace"
        repo_sub.mkdir()
        result = git_utils._resolve_git_diff_path("other/file.py", tmp_path, repo_sub)
        assert result is None


# =============================================================================
# Tests for _find_platform_item()
# =============================================================================


class TestFindPlatformItem:
    """Tests for .platform file discovery and parsing."""

    def test_finds_platform_in_same_directory(self, tmp_path):
        item_dir = tmp_path / "MyItem.Notebook"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            '{"metadata": {"type": "Notebook", "displayName": "MyItem"}}', encoding="utf-8"
        )
        file_path = item_dir / "notebook.py"
        file_path.touch()
        result = git_utils._find_platform_item(file_path, tmp_path)
        assert result == ("MyItem", "Notebook")

    def test_returns_none_when_no_platform_file(self, tmp_path):
        item_dir = tmp_path / "NoItem"
        item_dir.mkdir()
        file_path = item_dir / "file.py"
        file_path.touch()
        result = git_utils._find_platform_item(file_path, tmp_path)
        assert result is None

    def test_returns_none_for_malformed_platform_json(self, tmp_path):
        item_dir = tmp_path / "BadItem"
        item_dir.mkdir()
        (item_dir / ".platform").write_text("not valid json", encoding="utf-8")
        file_path = item_dir / "file.py"
        file_path.touch()
        result = git_utils._find_platform_item(file_path, tmp_path)
        assert result is None

    def test_returns_none_when_metadata_missing_type(self, tmp_path):
        item_dir = tmp_path / "NoType"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            '{"metadata": {"displayName": "NoType"}}', encoding="utf-8"
        )
        file_path = item_dir / "file.py"
        file_path.touch()
        result = git_utils._find_platform_item(file_path, tmp_path)
        assert result is None


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
        assert call_args == ["git", "diff", "--name-status", "main"]

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

    def test_deduplicates_multiple_files_in_same_item(self, tmp_path):
        """Multiple changed files in the same item should produce a single entry."""
        item_dir = tmp_path / "MyNotebook.Notebook"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            '{"metadata": {"type": "Notebook", "displayName": "MyNotebook"}}',
            encoding="utf-8",
        )
        (item_dir / "file1.py").write_text("a", encoding="utf-8")
        (item_dir / "file2.py").write_text("b", encoding="utf-8")

        diff_output = self._make_git_diff_output([
            "M\tMyNotebook.Notebook/file1.py",
            "M\tMyNotebook.Notebook/file2.py",
        ])

        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value.stdout = diff_output
            mock_run.return_value.returncode = 0
            result = git_utils.get_changed_items(tmp_path)

        assert result == ["MyNotebook.Notebook"]

    def test_handles_renamed_files(self, tmp_path):
        """Renamed files (R status) should be detected via the new path."""
        item_dir = tmp_path / "Renamed.Notebook"
        item_dir.mkdir()
        (item_dir / ".platform").write_text(
            '{"metadata": {"type": "Notebook", "displayName": "Renamed"}}',
            encoding="utf-8",
        )
        (item_dir / "new_name.py").write_text("x", encoding="utf-8")

        diff_output = self._make_git_diff_output(["R100\tOld.Notebook/old.py\tRenamed.Notebook/new_name.py"])

        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value.stdout = diff_output
            mock_run.return_value.returncode = 0
            result = git_utils.get_changed_items(tmp_path)

        assert result == ["Renamed.Notebook"]

    def test_returns_empty_list_on_timeout(self, tmp_path):
        """A git diff timeout should return an empty list gracefully."""
        import subprocess

        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run", side_effect=subprocess.TimeoutExpired("git", 30)),
        ):
            result = git_utils.get_changed_items(tmp_path)

        assert result == []

    def test_multiple_distinct_items(self, tmp_path):
        """Changes across multiple items should all be returned."""
        for name, item_type in [("NB1", "Notebook"), ("Pipeline1", "DataPipeline")]:
            item_dir = tmp_path / f"{name}.{item_type}"
            item_dir.mkdir()
            (item_dir / ".platform").write_text(
                f'{{"metadata": {{"type": "{item_type}", "displayName": "{name}"}}}}',
                encoding="utf-8",
            )
            (item_dir / "file.py").write_text("content", encoding="utf-8")

        diff_output = self._make_git_diff_output([
            "M\tNB1.Notebook/file.py",
            "A\tPipeline1.DataPipeline/file.py",
        ])

        git_root_patch = "fabric_cicd._common._config_validator._find_git_root"

        with (
            patch(git_root_patch, return_value=tmp_path),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value.stdout = diff_output
            mock_run.return_value.returncode = 0
            result = git_utils.get_changed_items(tmp_path)

        assert sorted(result) == ["NB1.Notebook", "Pipeline1.DataPipeline"]

    def test_rejects_dangerous_git_compare_ref(self, tmp_path):
        """Passing an invalid git_compare_ref should raise InputError before running git."""
        with pytest.raises(InputError):
            git_utils.get_changed_items(tmp_path, git_compare_ref="--exec=whoami")
