# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for shared custom exceptions."""

import logging

from fabric_cicd._common._exceptions import BaseCustomError, InvokeError, ItemCleanupError


def test_item_cleanup_error_preserves_failures_from_different_item_types():
    logger = logging.getLogger(__name__)
    resume_error = InvokeError("Resume failed", logger, "Resume request details")
    connection_error = RuntimeError("Connection close failed")
    errors = [
        ("Resume Eventstream source 'source-1'", resume_error),
        ("Close Notebook connection 'connection-1'", connection_error),
    ]

    error = ItemCleanupError(errors, logger)

    assert isinstance(error, BaseCustomError)
    assert error.errors == errors
    assert error.logger is logger
    assert "Failed to complete 2 item cleanup action(s)" in str(error)
    for label, original_error in errors:
        assert f"{label}: {original_error}" in str(error)
        assert f"--- {label} ---\n{original_error!s}" in error.additional_info
    assert "Resume request details" in error.additional_info
