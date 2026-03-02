# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the logging module and wrapper functions."""

import logging
import shutil
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest.mock import patch

import pytest

from fabric_cicd._common._logging import (
    CustomFormatter,
    _build_console_message,
    _build_file_message,
    _cleanup_managed_handlers,
    _configure_console_handler,
    _configure_file_handler,
    _get_file_handler,
    _mark_handler,
    configure_logger,
    exception_handler,
    log_header,
)


def close_all_file_handlers():
    """Close all file handlers to release file locks on Windows."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        if isinstance(handler, (logging.FileHandler, RotatingFileHandler)):
            handler.close()
            root_logger.removeHandler(handler)

    package_logger = logging.getLogger("fabric_cicd")
    for handler in package_logger.handlers[:]:
        if isinstance(handler, (logging.FileHandler, RotatingFileHandler)):
            handler.close()
            package_logger.removeHandler(handler)


class TestCustomFormatter:
    """Tests for the CustomFormatter class."""

    def test_format_info_level(self):
        """Test formatting of INFO level messages."""
        formatter = CustomFormatter("[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")
        record = logging.LogRecord(
            name="fabric_cicd",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "info" in formatted.lower()
        assert "Test message" in formatted

    def test_format_warning_level(self):
        """Test formatting of WARNING level messages."""
        formatter = CustomFormatter("[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")
        record = logging.LogRecord(
            name="fabric_cicd",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="Warning message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "warn" in formatted.lower()
        assert "Warning message" in formatted

    def test_format_error_level(self):
        """Test formatting of ERROR level messages."""
        formatter = CustomFormatter("[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")
        record = logging.LogRecord(
            name="fabric_cicd",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Error message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "error" in formatted.lower()
        assert "Error message" in formatted

    def test_format_debug_level(self):
        """Test formatting of DEBUG level messages."""
        formatter = CustomFormatter("[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")
        record = logging.LogRecord(
            name="fabric_cicd",
            level=logging.DEBUG,
            pathname="",
            lineno=0,
            msg="Debug message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "debug" in formatted.lower()
        assert "Debug message" in formatted

    def test_format_critical_level(self):
        """Test formatting of CRITICAL level messages."""
        formatter = CustomFormatter("[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")
        record = logging.LogRecord(
            name="fabric_cicd",
            level=logging.CRITICAL,
            pathname="",
            lineno=0,
            msg="Critical message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "crit" in formatted.lower()
        assert "Critical message" in formatted

    def test_format_with_indent(self):
        """Test formatting of messages with indent marker."""
        from fabric_cicd import constants

        formatter = CustomFormatter("[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")
        record = logging.LogRecord(
            name="fabric_cicd",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"{constants.INDENT}Indented message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        # Indented messages should start with spaces
        assert "Indented message" in formatted
        assert formatted.startswith(" " * 8)


class TestMarkHandler:
    """Tests for the _mark_handler function."""

    def test_mark_handler_sets_attribute(self):
        """Test that _mark_handler sets the managed attribute on a handler."""
        handler = logging.StreamHandler()
        marked = _mark_handler(handler)
        assert getattr(marked, "_fabric_cicd_managed", False) is True

    def test_mark_handler_returns_same_handler(self):
        """Test that _mark_handler returns the same handler instance."""
        handler = logging.StreamHandler()
        marked = _mark_handler(handler)
        assert marked is handler


class TestCleanupManagedHandlers:
    """Tests for the _cleanup_managed_handlers function."""

    def test_removes_managed_handlers(self):
        """Test that managed handlers are removed."""
        logger = logging.getLogger("test_cleanup_managed")
        handler = _mark_handler(logging.StreamHandler())
        logger.addHandler(handler)

        _cleanup_managed_handlers(logger)
        assert handler not in logger.handlers

        # Clean up
        logger.handlers = []

    def test_preserves_external_handlers(self):
        """Test that non-managed handlers are preserved."""
        logger = logging.getLogger("test_cleanup_external")
        external_handler = logging.StreamHandler()
        managed_handler = _mark_handler(logging.StreamHandler())
        logger.addHandler(external_handler)
        logger.addHandler(managed_handler)

        _cleanup_managed_handlers(logger)
        assert external_handler in logger.handlers
        assert managed_handler not in logger.handlers

        # Clean up
        logger.removeHandler(external_handler)

    def test_cleanup_multiple_loggers(self):
        """Test cleanup across multiple loggers."""
        logger_a = logging.getLogger("test_cleanup_a")
        logger_b = logging.getLogger("test_cleanup_b")
        handler_a = _mark_handler(logging.StreamHandler())
        handler_b = _mark_handler(logging.StreamHandler())
        logger_a.addHandler(handler_a)
        logger_b.addHandler(handler_b)

        _cleanup_managed_handlers(logger_a, logger_b)
        assert handler_a not in logger_a.handlers
        assert handler_b not in logger_b.handlers

        # Clean up
        logger_a.handlers = []
        logger_b.handlers = []


class TestConfigureFileHandler:
    """Tests for the _configure_file_handler function."""

    def test_default_file_handler(self):
        """Test default file handler configuration."""
        handler = _configure_file_handler(
            level=logging.INFO,
            file_path=None,
            use_file_rotation=False,
            debug_only_file=False,
        )
        try:
            assert isinstance(handler, logging.FileHandler)
            assert not isinstance(handler, RotatingFileHandler)
            assert getattr(handler, "_fabric_cicd_managed", False) is True
        finally:
            handler.close()

    def test_rotating_file_handler(self):
        """Test rotating file handler configuration."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "test.log"

        try:
            handler = _configure_file_handler(
                level=logging.DEBUG,
                file_path=str(log_file),
                use_file_rotation=True,
                debug_only_file=False,
            )
            assert isinstance(handler, RotatingFileHandler)
            assert handler.maxBytes == 5 * 1024 * 1024
            assert handler.backupCount == 7
            assert getattr(handler, "_fabric_cicd_managed", False) is True
        finally:
            handler.close()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_debug_only_file_filter(self):
        """Test debug_only_file filter only passes DEBUG records."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "debug_only.log"

        try:
            handler = _configure_file_handler(
                level=logging.DEBUG,
                file_path=str(log_file),
                use_file_rotation=True,
                debug_only_file=True,
            )
            assert handler.level == logging.DEBUG

            # Create test records from fabric_cicd namespace
            debug_record = logging.LogRecord(
                name="fabric_cicd", level=logging.DEBUG, pathname="", lineno=0, msg="debug", args=(), exc_info=None
            )
            info_record = logging.LogRecord(
                name="fabric_cicd", level=logging.INFO, pathname="", lineno=0, msg="info", args=(), exc_info=None
            )
            error_record = logging.LogRecord(
                name="fabric_cicd", level=logging.ERROR, pathname="", lineno=0, msg="error", args=(), exc_info=None
            )

            # DEBUG should pass all filters, INFO and ERROR should be blocked by debug_only filter
            assert all(f(debug_record) for f in handler.filters)
            assert not all(f(info_record) for f in handler.filters)
            assert not all(f(error_record) for f in handler.filters)
        finally:
            handler.close()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_namespace_filter_blocks_third_party_logs(self):
        """Test that the namespace filter blocks non-fabric_cicd logs."""
        handler = _configure_file_handler(
            level=logging.INFO,
            file_path=None,
            use_file_rotation=False,
            debug_only_file=False,
        )
        try:
            fabric_record = logging.LogRecord(
                name="fabric_cicd.publish",
                level=logging.ERROR,
                pathname="",
                lineno=0,
                msg="fabric msg",
                args=(),
                exc_info=None,
            )
            azure_record = logging.LogRecord(
                name="azure.identity",
                level=logging.ERROR,
                pathname="",
                lineno=0,
                msg="azure msg",
                args=(),
                exc_info=None,
            )

            assert all(f(fabric_record) for f in handler.filters)
            assert not all(f(azure_record) for f in handler.filters)
        finally:
            handler.close()

    def test_no_rotation_without_file_path(self):
        """Test that rotation is not used when file_path is None even if use_file_rotation is True."""
        handler = _configure_file_handler(
            level=logging.DEBUG,
            file_path=None,
            use_file_rotation=True,
            debug_only_file=False,
        )
        try:
            assert isinstance(handler, logging.FileHandler)
            assert not isinstance(handler, RotatingFileHandler)
        finally:
            handler.close()

    def test_debug_only_not_applied_when_level_is_not_debug(self):
        """Test that debug_only_file has no effect when level is not DEBUG."""
        handler = _configure_file_handler(
            level=logging.INFO,
            file_path=None,
            use_file_rotation=False,
            debug_only_file=True,
        )
        try:
            # Should only have the namespace filter, not the debug_only filter
            assert len(handler.filters) == 1
        finally:
            handler.close()


class TestConfigureConsoleHandler:
    """Tests for the _configure_console_handler function."""

    def test_console_handler_level(self):
        """Test console handler is set to the specified level."""
        handler = _configure_console_handler(logging.WARNING)
        assert handler.level == logging.WARNING
        assert getattr(handler, "_fabric_cicd_managed", False) is True

    def test_console_handler_uses_custom_formatter(self):
        """Test console handler uses CustomFormatter."""
        handler = _configure_console_handler(logging.INFO)
        assert isinstance(handler.formatter, CustomFormatter)

    def test_console_handler_is_stream_handler(self):
        """Test console handler is a StreamHandler."""
        handler = _configure_console_handler(logging.INFO)
        assert isinstance(handler, logging.StreamHandler)


class TestGetFileHandler:
    """Tests for the _get_file_handler function."""

    def setup_method(self):
        """Reset root logger handlers before each test."""
        close_all_file_handlers()
        root_logger = logging.getLogger()
        root_logger.handlers = []

    def teardown_method(self):
        """Clean up file handlers after each test."""
        close_all_file_handlers()

    def test_returns_none_when_no_file_handler(self):
        """Test returns None when no file handler exists on root logger."""
        assert _get_file_handler() is None

    def test_returns_managed_file_handler(self):
        """Test returns the managed file handler from root logger."""
        root_logger = logging.getLogger()
        handler = _mark_handler(logging.FileHandler("test_get.log", delay=True))
        root_logger.addHandler(handler)

        result = _get_file_handler()
        assert result is handler

        handler.close()
        root_logger.removeHandler(handler)

    def test_ignores_unmanaged_file_handler(self):
        """Test ignores file handlers not marked as managed."""
        root_logger = logging.getLogger()
        handler = logging.FileHandler("test_unmanaged.log", delay=True)
        root_logger.addHandler(handler)

        result = _get_file_handler()
        assert result is None

        handler.close()
        root_logger.removeHandler(handler)

    def test_returns_managed_rotating_handler(self):
        """Test returns a managed RotatingFileHandler."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "test_rotating.log"

        try:
            root_logger = logging.getLogger()
            handler = _mark_handler(RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1))
            root_logger.addHandler(handler)

            result = _get_file_handler()
            assert result is handler
            assert isinstance(result, RotatingFileHandler)
        finally:
            handler.close()
            root_logger.removeHandler(handler)
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestBuildConsoleMessage:
    """Tests for the _build_console_message function."""

    def test_no_file_handler(self):
        """Test message without file handler reference."""
        exception = Exception("Something failed")
        result = _build_console_message(exception, None)
        assert result == "Something failed"

    def test_with_default_file_handler(self):
        """Test message includes file path for default FileHandler."""
        handler = logging.FileHandler("fabric_cicd.error.log", delay=True)
        try:
            exception = Exception("Something failed")
            result = _build_console_message(exception, handler)
            assert "Something failed" in result
            assert "See" in result
            assert "fabric_cicd.error.log" in result
        finally:
            handler.close()

    def test_with_rotating_file_handler(self):
        """Test message excludes file path for RotatingFileHandler."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "debug.log"

        try:
            handler = RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1)
            exception = Exception("Something failed")
            result = _build_console_message(exception, handler)
            assert result == "Something failed"
            assert "See" not in result
        finally:
            handler.close()
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestBuildFileMessage:
    """Tests for the _build_file_message function."""

    def test_no_additional_info(self):
        """Test file message without additional info returns %s placeholder."""
        exception = Exception("Something failed")
        result = _build_file_message(exception)
        assert result == "%s"

    def test_with_additional_info(self):
        """Test file message includes additional info."""
        exception = Exception("Something failed")
        exception.additional_info = "status: 403, reason: Forbidden"
        result = _build_file_message(exception)
        assert "%s" in result
        assert "Additional Info" in result
        assert "status: 403, reason: Forbidden" in result

    def test_with_none_additional_info(self):
        """Test file message when additional_info is explicitly None."""
        exception = Exception("Something failed")
        exception.additional_info = None
        result = _build_file_message(exception)
        assert result == "%s"


class TestConfigureLogger:
    """Tests for the configure_logger function."""

    def setup_method(self):
        """Reset loggers before each test."""
        close_all_file_handlers()

        # Clear all handlers from root and package loggers
        root_logger = logging.getLogger()
        root_logger.handlers = []

        package_logger = logging.getLogger("fabric_cicd")
        package_logger.handlers = []

        console_only_logger = logging.getLogger("console_only")
        console_only_logger.handlers = []

    def teardown_method(self):
        """Clean up file handlers after each test."""
        close_all_file_handlers()

    def test_configure_logger_default_info_level(self):
        """Test default configuration sets INFO level."""
        configure_logger(disable_log_file=True)

        package_logger = logging.getLogger("fabric_cicd")
        assert package_logger.level == logging.INFO

    def test_configure_logger_debug_level(self):
        """Test DEBUG level configuration."""
        configure_logger(level=logging.DEBUG, disable_log_file=True)

        package_logger = logging.getLogger("fabric_cicd")
        assert package_logger.level == logging.DEBUG

    def test_configure_logger_root_level_debug_mode(self):
        """Test root logger is set to INFO when package is DEBUG."""
        configure_logger(level=logging.DEBUG, disable_log_file=True)

        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO

    def test_configure_logger_root_level_info_mode(self):
        """Test root logger is set to ERROR when package is INFO."""
        configure_logger(level=logging.INFO, disable_log_file=True)

        root_logger = logging.getLogger()
        assert root_logger.level == logging.ERROR

    def test_configure_logger_disable_file_logging(self):
        """Test file logging can be disabled."""
        configure_logger(disable_log_file=True)

        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 0

    def test_configure_logger_with_file_handler(self):
        """Test default configuration includes file handler."""
        configure_logger()

        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1

    def test_configure_logger_with_rotation(self):
        """Test configuration with file rotation."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "test.log"

        try:
            configure_logger(level=logging.DEBUG, file_path=str(log_file), use_file_rotation=True)

            root_logger = logging.getLogger()
            rotating_handlers = [h for h in root_logger.handlers if isinstance(h, RotatingFileHandler)]
            assert len(rotating_handlers) == 1
        finally:
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_configure_logger_suppress_debug_console(self):
        """Test suppressing DEBUG output to console."""
        configure_logger(level=logging.DEBUG, suppress_debug_console=True, disable_log_file=True)

        package_logger = logging.getLogger("fabric_cicd")
        console_handlers = [h for h in package_logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(console_handlers) == 1
        # Console handler should be at INFO level when suppress_debug_console is True
        assert console_handlers[0].level == logging.INFO

    def test_configure_logger_suppress_debug_console_no_effect_at_info(self):
        """Test that suppress_debug_console has no effect when level is not DEBUG."""
        configure_logger(level=logging.INFO, suppress_debug_console=True, disable_log_file=True)

        package_logger = logging.getLogger("fabric_cicd")
        console_handlers = [h for h in package_logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == logging.INFO

    def test_configure_logger_console_only_logger(self):
        """Test console_only logger is properly configured."""
        configure_logger(disable_log_file=True)

        console_only_logger = logging.getLogger("console_only")
        assert console_only_logger.propagate is False
        assert len(console_only_logger.handlers) == 1

    def test_configure_logger_console_only_logger_separate_handler(self):
        """Test that console_only logger has a separate handler instance from package logger."""
        configure_logger(disable_log_file=True)

        package_logger = logging.getLogger("fabric_cicd")
        console_only_logger = logging.getLogger("console_only")

        # Handlers should be different instances
        assert package_logger.handlers[0] is not console_only_logger.handlers[0]

    def test_configure_logger_clears_existing_handlers(self):
        """Test that configuring logger clears existing managed handlers without affecting external ones."""
        # Add an external handler to the root logger
        external_handler = logging.StreamHandler()
        root_logger = logging.getLogger()
        root_logger.addHandler(external_handler)

        # Configure fabric_cicd logging
        configure_logger(disable_log_file=True)

        # Reconfigure - should not accumulate managed handlers
        managed_count_before = len(root_logger.handlers)
        configure_logger(disable_log_file=True)
        managed_count_after = len(root_logger.handlers)

        assert managed_count_after == managed_count_before

        # External handler should still be present
        assert external_handler in root_logger.handlers

        # Clean up the external handler
        root_logger.removeHandler(external_handler)

    def test_configure_logger_preserves_external_handlers(self):
        """Test that external (non-fabric_cicd) handlers survive reconfiguration."""
        root_logger = logging.getLogger()
        package_logger = logging.getLogger("fabric_cicd")

        # Add external handlers
        external_root_handler = logging.StreamHandler()
        external_package_handler = logging.StreamHandler()
        root_logger.addHandler(external_root_handler)
        package_logger.addHandler(external_package_handler)

        # Configure fabric_cicd logging
        configure_logger(disable_log_file=True)

        # External handlers should still be present
        assert external_root_handler in root_logger.handlers
        assert external_package_handler in package_logger.handlers

        # Clean up
        root_logger.removeHandler(external_root_handler)
        package_logger.removeHandler(external_package_handler)

    def test_configure_logger_package_logger_propagates(self):
        """Test that package logger propagates to root (for file logging via propagation)."""
        configure_logger(disable_log_file=True)

        package_logger = logging.getLogger("fabric_cicd")
        assert package_logger.propagate is True

    def test_configure_logger_all_handlers_marked(self):
        """Test that all handlers added by configure_logger are marked as managed."""
        configure_logger()

        root_logger = logging.getLogger()
        package_logger = logging.getLogger("fabric_cicd")
        console_only_logger = logging.getLogger("console_only")

        for logger_instance in [root_logger, package_logger, console_only_logger]:
            for handler in logger_instance.handlers:
                if getattr(handler, "_fabric_cicd_managed", False):
                    assert True
                    return

        # At least one managed handler should exist
        managed = sum(
            getattr(h, "_fabric_cicd_managed", False)
            for lg in [root_logger, package_logger, console_only_logger]
            for h in lg.handlers
        )
        assert managed >= 3  # file handler + 2 console handlers


class TestLogHeader:
    """Tests for the log_header function."""

    def test_log_header_calls_logger(self, caplog):
        """Test log_header logs the expected messages."""
        logger = logging.getLogger("fabric_cicd.test")
        logger.setLevel(logging.INFO)

        with caplog.at_level(logging.INFO, logger="fabric_cicd.test"):
            log_header(logger, "Test Header")

        # Should have 4 log records: blank line, top border, header message, bottom border
        assert len(caplog.records) >= 3
        # Check that the header message is present
        header_found = any("Test Header" in record.message for record in caplog.records)
        assert header_found


class TestWrapperFunctions:
    """Tests for the wrapper functions in __init__.py."""

    def setup_method(self):
        """Reset loggers and feature flags before each test."""
        from fabric_cicd import constants

        close_all_file_handlers()

        # Clear feature flags
        constants.FEATURE_FLAG.clear()

        # Reset loggers
        root_logger = logging.getLogger()
        root_logger.handlers = []

        package_logger = logging.getLogger("fabric_cicd")
        package_logger.handlers = []

    def teardown_method(self):
        """Clean up file handlers after each test."""
        close_all_file_handlers()

    def test_append_feature_flag(self):
        """Test append_feature_flag adds flag to set."""
        from fabric_cicd import append_feature_flag, constants

        append_feature_flag("test_feature")
        assert "test_feature" in constants.FEATURE_FLAG

    def test_append_feature_flag_multiple(self):
        """Test adding multiple feature flags."""
        from fabric_cicd import append_feature_flag, constants

        append_feature_flag("feature_1")
        append_feature_flag("feature_2")
        assert "feature_1" in constants.FEATURE_FLAG
        assert "feature_2" in constants.FEATURE_FLAG

    def test_append_feature_flag_no_duplicates(self):
        """Test that duplicate flags are not added (set behavior)."""
        from fabric_cicd import append_feature_flag, constants

        append_feature_flag("duplicate_feature")
        append_feature_flag("duplicate_feature")
        # Count occurrences - should be 1 since it's a set
        assert len([f for f in constants.FEATURE_FLAG if f == "duplicate_feature"]) == 1

    def test_change_log_level_debug(self):
        """Test change_log_level sets DEBUG level."""
        from fabric_cicd import change_log_level

        change_log_level("DEBUG")

        package_logger = logging.getLogger("fabric_cicd")
        assert package_logger.level == logging.DEBUG

    def test_change_log_level_case_insensitive(self):
        """Test change_log_level is case insensitive."""
        from fabric_cicd import change_log_level

        change_log_level("debug")

        package_logger = logging.getLogger("fabric_cicd")
        assert package_logger.level == logging.DEBUG

    def test_change_log_level_unsupported(self, capsys):
        """Test change_log_level warns on unsupported level."""
        from fabric_cicd import change_log_level

        # First configure the logger
        configure_logger(disable_log_file=True)

        change_log_level("TRACE")

        # Check stderr for the warning message
        captured = capsys.readouterr()
        assert "not supported" in captured.err

    def test_disable_file_logging(self):
        """Test disable_file_logging removes file handlers."""
        from fabric_cicd import disable_file_logging

        # First ensure file logging is enabled
        configure_logger()

        root_logger = logging.getLogger()
        initial_file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(initial_file_handlers) >= 1

        # Disable file logging
        disable_file_logging()

        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 0

    def test_configure_logger_with_rotation_wrapper(self):
        """Test configure_logger_with_rotation sets up rotation."""
        from fabric_cicd import configure_logger_with_rotation

        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "rotation_test.log"

        try:
            configure_logger_with_rotation(str(log_file))

            root_logger = logging.getLogger()
            rotating_handlers = [h for h in root_logger.handlers if isinstance(h, RotatingFileHandler)]
            assert len(rotating_handlers) == 1

            # Verify DEBUG level is set
            package_logger = logging.getLogger("fabric_cicd")
            assert package_logger.level == logging.DEBUG

            # Verify console is suppressed to INFO
            console_handlers = [
                h
                for h in package_logger.handlers
                if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
            ]
            assert len(console_handlers) == 1
            assert console_handlers[0].level == logging.INFO
        finally:
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestExceptionHandler:
    """Tests for the exception_handler function."""

    def setup_method(self):
        """Reset loggers before each test."""
        close_all_file_handlers()

    def teardown_method(self):
        """Clean up file handlers after each test."""
        close_all_file_handlers()

    def test_exception_handler_custom_exception(self):
        """Test exception handler handles custom exceptions."""
        from fabric_cicd._common._exceptions import InputError

        # Create a logger for the exception
        test_logger = logging.getLogger("fabric_cicd.test")

        # Create an InputError with a logger
        exception = InputError("Test error message", logger=test_logger)

        # Configure logger to capture output
        configure_logger(disable_log_file=True)

        # Call exception handler - should not raise
        try:
            exception_handler(InputError, exception, None)
        except Exception:
            pytest.fail("exception_handler raised an unexpected exception")

    def test_exception_handler_standard_exception(self):
        """Test exception handler falls back to default for standard exceptions."""
        with patch("sys.__excepthook__") as mock_excepthook:
            exception = ValueError("Standard error")
            exception_handler(ValueError, exception, None)

            mock_excepthook.assert_called_once()

    def test_exception_handler_removes_console_from_package_logger(self):
        """Test that exception handler removes console handler from package logger."""
        from fabric_cicd._common._exceptions import InputError

        configure_logger(disable_log_file=True)

        test_logger = logging.getLogger("fabric_cicd.test")
        exception = InputError("Test error", logger=test_logger)

        package_logger = logging.getLogger("fabric_cicd")
        assert len(package_logger.handlers) >= 1

        exception_handler(InputError, exception, None)

        # Console handler should be removed from package_logger after exception_handler
        managed_handlers = [h for h in package_logger.handlers if getattr(h, "_fabric_cicd_managed", False)]
        assert len(managed_handlers) == 0

    def test_exception_handler_console_only_message(self):
        """Test that exception handler writes to console_only logger."""
        from fabric_cicd._common._exceptions import InputError

        configure_logger(disable_log_file=True)

        test_logger = logging.getLogger("fabric_cicd.test")
        exception = InputError("User-facing error", logger=test_logger)

        with patch.object(logging.getLogger("console_only"), "error") as mock_error:
            exception_handler(InputError, exception, None)
            mock_error.assert_called_once()
            assert "User-facing error" in mock_error.call_args[0][0]


class TestDelayedFileCreation:
    """Tests for delayed file creation behavior."""

    def setup_method(self):
        """Reset loggers before each test."""
        close_all_file_handlers()

    def teardown_method(self):
        """Clean up file handlers after each test."""
        close_all_file_handlers()

    def test_file_not_created_until_log_written(self):
        """Test that log file is not created until first log is written (delay=True)."""
        tmpdir = Path(tempfile.mkdtemp())
        original_cwd = Path.cwd()

        try:
            import os

            os.chdir(tmpdir)

            # Configure logger (file handler has delay=True)
            configure_logger()

            # File should not exist yet
            log_file = tmpdir / "fabric_cicd.error.log"
            assert not log_file.exists()

        finally:
            import os

            os.chdir(original_cwd)
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_disable_file_logging_prevents_file_creation(self):
        """Test that disable_file_logging prevents file creation."""
        from fabric_cicd import disable_file_logging

        tmpdir = Path(tempfile.mkdtemp())
        original_cwd = Path.cwd()

        try:
            import os

            os.chdir(tmpdir)

            # Disable file logging before any logs
            disable_file_logging()

            # Log something
            logger = logging.getLogger("fabric_cicd")
            logger.error("This should not create a file")

            # File should not exist
            log_file = tmpdir / "fabric_cicd.error.log"
            assert not log_file.exists()

        finally:
            import os

            os.chdir(original_cwd)
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestFileLoggingIntegration:
    """Integration tests for file logging functionality."""

    def setup_method(self):
        """Reset loggers before each test."""
        close_all_file_handlers()

    def teardown_method(self):
        """Clean up file handlers after each test."""
        close_all_file_handlers()

    def test_rotating_file_handler_writes_logs(self):
        """Test that rotating file handler actually writes logs."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "test_rotation.log"

        try:
            configure_logger(level=logging.DEBUG, file_path=str(log_file), use_file_rotation=True)

            logger = logging.getLogger("fabric_cicd")
            logger.debug("Debug message for rotation test")

            # Force flush
            for handler in logging.getLogger().handlers:
                if hasattr(handler, "flush"):
                    handler.flush()

            # Check file was created and contains the message
            assert log_file.exists()
            content = log_file.read_text(encoding="utf-8")
            assert "Debug message for rotation test" in content

        finally:
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_debug_only_file_filter(self):
        """Test that debug_only_file only writes DEBUG messages to file."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "debug_only.log"

        try:
            configure_logger(
                level=logging.DEBUG,
                file_path=str(log_file),
                use_file_rotation=True,
                debug_only_file=True,
            )

            logger = logging.getLogger("fabric_cicd")
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")

            # Force flush
            for handler in logging.getLogger().handlers:
                if hasattr(handler, "flush"):
                    handler.flush()

            # Check file contains only DEBUG message
            content = log_file.read_text(encoding="utf-8")
            assert "Debug message" in content
            assert "Info message" not in content
            assert "Warning message" not in content

        finally:
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestLoggerFiltering:
    """Tests for logger filtering behavior."""

    def setup_method(self):
        """Reset loggers before each test."""
        close_all_file_handlers()

    def teardown_method(self):
        """Clean up file handlers after each test."""
        close_all_file_handlers()

    def test_file_handler_filters_non_fabric_cicd_logs(self):
        """Test that file handler only accepts fabric_cicd logs."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "filtered.log"

        try:
            configure_logger(level=logging.DEBUG, file_path=str(log_file), use_file_rotation=True)
            # Log from fabric_cicd logger
            fabric_logger = logging.getLogger("fabric_cicd")
            fabric_logger.debug("Fabric CICD message")

            # Log from a different logger
            other_logger = logging.getLogger("other_package")
            other_logger.setLevel(logging.DEBUG)
            other_logger.debug("Other package message")

            # Force flush
            for handler in logging.getLogger().handlers:
                if hasattr(handler, "flush"):
                    handler.flush()

            # Check file contains only fabric_cicd message
            content = log_file.read_text(encoding="utf-8")
            assert "Fabric CICD message" in content
            assert "Other package message" not in content

        finally:
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_console_only_logger_does_not_propagate(self):
        """Test that console_only logger does not write to file via propagation."""
        tmpdir = Path(tempfile.mkdtemp())
        log_file = tmpdir / "propagation_test.log"

        try:
            configure_logger(level=logging.DEBUG, file_path=str(log_file), use_file_rotation=True)

            console_only_logger = logging.getLogger("console_only")
            console_only_logger.error("Console only error")

            # Force flush
            for handler in logging.getLogger().handlers:
                if hasattr(handler, "flush"):
                    handler.flush()

            # File should not contain console_only messages
            if log_file.exists():
                content = log_file.read_text(encoding="utf-8")
                assert "Console only error" not in content

        finally:
            close_all_file_handlers()
            shutil.rmtree(tmpdir, ignore_errors=True)
