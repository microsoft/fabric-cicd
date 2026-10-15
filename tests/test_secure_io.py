# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for _secure_io module."""

import json
import os
import stat
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from fabric_cicd._common._secure_io import IS_POSIX, OWNER_ONLY_FILE_MODE, restrict_file, restricted_opener

posix_only = pytest.mark.skipif(not IS_POSIX, reason="POSIX permission bits are a no-op on Windows")


def _mode(path: str) -> int:
    return stat.S_IMODE(Path(path).stat().st_mode)


# ---------------------------------------------------------------------------
# restrict_file
# ---------------------------------------------------------------------------


@posix_only
def test_restrict_file_tightens_world_readable():
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        f.write("sensitive data")
        path = f.name
    try:
        Path(path).chmod(0o644)
        restrict_file(path)
        assert _mode(path) == OWNER_ONLY_FILE_MODE
    finally:
        Path(path).unlink()


@posix_only
def test_restrict_file_already_restricted():
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        f.write("data")
        path = f.name
    try:
        Path(path).chmod(0o600)
        restrict_file(path)
        assert _mode(path) == OWNER_ONLY_FILE_MODE
    finally:
        Path(path).unlink()


def test_restrict_file_missing_file_is_noop():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "does-not-exist.json")
        restrict_file(path)
        assert not Path(path).exists()


def test_restrict_file_noop_on_windows():
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        f.write("data")
        path = f.name
    try:
        import fabric_cicd._common._secure_io as mod

        with patch.object(mod, "IS_POSIX", False), patch("pathlib.Path.chmod") as mock_chmod:
            mod.restrict_file(path)
            mock_chmod.assert_not_called()
    finally:
        Path(path).unlink()


# ---------------------------------------------------------------------------
# restricted_opener
# ---------------------------------------------------------------------------


@posix_only
def test_restricted_opener_creates_file_with_owner_only_perms():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "new_file.json")
        with open(path, "w", opener=restricted_opener) as f:
            f.write("data")
        assert _mode(path) == OWNER_ONLY_FILE_MODE


@posix_only
def test_restricted_opener_with_restrict_file_tightens_existing():
    """Test the production pattern: restrict_file() + open(opener=restricted_opener)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "existing.json")
        Path(path).write_text("old")
        Path(path).chmod(0o644)
        # Production pattern: tighten first, then write with restricted opener
        restrict_file(path)
        with open(path, "w", opener=restricted_opener) as f:
            f.write("new")
        assert Path(path).read_text() == "new"
        assert _mode(path) == OWNER_ONLY_FILE_MODE


# ---------------------------------------------------------------------------
# FileTracer.save() integration
# ---------------------------------------------------------------------------


@posix_only
def test_file_tracer_save_creates_restricted_trace_file():
    from fabric_cicd._common._http_tracer import FileTracer

    with tempfile.TemporaryDirectory() as tmpdir:
        original_cwd = Path.cwd()
        os.chdir(tmpdir)
        try:
            trace_path = str(Path(tmpdir) / "trace.json")
            tracer = FileTracer(output_file=trace_path)
            tracer.capture_request(
                method="GET",
                url="https://api.fabric.microsoft.com/v1/workspaces",
                headers={"Content-Type": "application/json"},
                body="",
                files=None,
            )
            tracer.save()

            assert Path(trace_path).exists()
            assert _mode(trace_path) == OWNER_ONLY_FILE_MODE

            data = json.loads(Path(trace_path).read_text())
            assert data["total_traces"] == 1
            assert data["traces"][0]["request"]["method"] == "GET"
        finally:
            os.chdir(original_cwd)


def test_file_tracer_save_writes_valid_json():
    from fabric_cicd._common._http_tracer import FileTracer

    with tempfile.TemporaryDirectory() as tmpdir:
        original_cwd = Path.cwd()
        os.chdir(tmpdir)
        try:
            trace_path = str(Path(tmpdir) / "trace.json")
            tracer = FileTracer(output_file=trace_path)
            tracer.capture_request(
                method="POST",
                url="https://api.fabric.microsoft.com/v1/items",
                headers={"Content-Type": "application/json"},
                body='{"displayName": "test"}',
                files=None,
            )
            tracer.save()

            data = json.loads(Path(trace_path).read_text())
            assert data["total_traces"] == 1
            assert data["traces"][0]["request"]["url"] == "https://api.fabric.microsoft.com/v1/items"
        finally:
            os.chdir(original_cwd)
