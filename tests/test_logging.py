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
            configure_logger(level=logging.DEBUG, file_path=str(log_file), rotate_on=True)

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

    def test_configure_logger_console_only_logger(self):
        """Test console_only logger is properly configured."""
        configure_logger(disable_log_file=True)

        console_only_logger = logging.getLogger("console_only")
        assert console_only_logger.propagate is False
        assert len(console_only_logger.handlers) == 1

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
            configure_logger(level=logging.DEBUG, file_path=str(log_file), rotate_on=True)

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
                rotate_on=True,
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
            configure_logger(level=logging.DEBUG, file_path=str(log_file), rotate_on=True)

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
