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

from fabric_cicd import (
    append_feature_flag,
    change_log_level,
    configure_external_file_logging,
    constants,
    disable_file_logging,
)
from fabric_cicd._common._logging import (
    CustomFormatter,
    PackageFilter,
    _build_console_message,
    _build_file_message,
    _cleanup_managed_handlers,
    _configure_console_handler,
    _configure_default_file_handler,
    _configure_external_file_handler,
    _mark_external_handler,
    _mark_handler,
    configure_logger,
    exception_handler,
    get_file_handler,
    log_header,
)


def _close_all_file_handlers():
    """Close all file handlers to release file locks on Windows."""
    for logger_name in ("", "fabric_cicd"):
        logger = logging.getLogger(logger_name)
        for handler in logger.handlers[:]:
            if isinstance(handler, (logging.FileHandler, RotatingFileHandler)):
                handler.close()
                logger.removeHandler(handler)


def _reset_logger(logger_name: str) -> None:
    """Reset a logger to clean state."""
    logger = logging.getLogger(logger_name)
    logger.handlers = []


@pytest.fixture(autouse=True)
def _clean_logging_state():
    """Reset logging state before and after each test to release file locks on Windows."""
    _close_all_file_handlers()
    for logger_name in ("", "fabric_cicd", "console_only"):
        _reset_logger(logger_name)

    yield

    _close_all_file_handlers()


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files."""
    tmpdir = Path(tempfile.mkdtemp())
    yield tmpdir
    _close_all_file_handlers()
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def external_rotating_handler(temp_log_dir):
    """Create an external RotatingFileHandler for testing."""
    log_file = temp_log_dir / "external.log"
    handler = RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    yield handler
    handler.close()


@pytest.fixture
def external_logger_with_handler(temp_log_dir):
    """Create an external logger with a RotatingFileHandler attached."""
    log_file = temp_log_dir / "external.log"
    handler = RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1)
    handler.setFormatter(logging.Formatter("%(message)s"))

    logger = logging.getLogger(f"ExternalLogger_{id(handler)}")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    yield logger, handler, log_file

    handler.close()
    logger.removeHandler(handler)


class TestCustomFormatter:
    """Tests for the CustomFormatter class."""

    @pytest.mark.parametrize(
        ("level", "level_name", "message"),
        [
            (logging.DEBUG, "debug", "Debug message"),
            (logging.INFO, "info", "Info message"),
            (logging.WARNING, "warn", "Warning message"),
            (logging.ERROR, "error", "Error message"),
            (logging.CRITICAL, "crit", "Critical message"),
        ],
    )
    def test_format_levels(self, level, level_name, message):
        """Test formatting of various log levels."""
        formatter = CustomFormatter("[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")
        record = logging.LogRecord(
            name="fabric_cicd",
            level=level,
            pathname="",
            lineno=0,
            msg=message,
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert level_name in formatted.lower()
        assert message in formatted

    def test_format_with_indent(self):
        """Test formatting of messages with indent marker."""

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
        assert "Indented message" in formatted
        assert formatted.startswith(" " * 8)


class TestPackageFilter:
    """Tests for the PackageFilter class."""

    @pytest.mark.parametrize(
        ("logger_name", "expected"),
        [
            ("fabric_cicd", True),
            ("fabric_cicd.publish", True),
            ("fabric_cicd._common._logging", True),
            ("azure.identity", False),
            ("urllib3.connectionpool", False),
            ("other_package", False),
        ],
    )
    def test_namespace_filtering(self, logger_name, expected):
        """Test filter correctly handles fabric_cicd and third-party namespaces."""
        filter_instance = PackageFilter()
        record = logging.LogRecord(
            name=logger_name, level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        assert filter_instance.filter(record) is expected

    @pytest.mark.parametrize(
        ("level", "expected"),
        [
            (logging.DEBUG, True),
            (logging.INFO, False),
            (logging.WARNING, False),
            (logging.ERROR, False),
            (logging.CRITICAL, False),
        ],
    )
    def test_debug_only_mode(self, level, expected):
        """Test debug_only=True only allows DEBUG level from fabric_cicd."""
        filter_instance = PackageFilter(debug_only=True)
        record = logging.LogRecord(
            name="fabric_cicd", level=level, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        assert filter_instance.filter(record) is expected

    def test_debug_only_still_checks_namespace(self):
        """Test debug_only=True still blocks non-fabric_cicd DEBUG logs."""
        filter_instance = PackageFilter(debug_only=True)
        record = logging.LogRecord(
            name="azure.identity", level=logging.DEBUG, pathname="", lineno=0, msg="debug", args=(), exc_info=None
        )
        assert filter_instance.filter(record) is False

    def test_default_allows_all_levels_from_package(self):
        """Test default filter (debug_only=False) allows all levels from fabric_cicd."""
        filter_instance = PackageFilter(debug_only=False)
        levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
        for level in levels:
            record = logging.LogRecord(
                name="fabric_cicd", level=level, pathname="", lineno=0, msg="test", args=(), exc_info=None
            )
            assert filter_instance.filter(record) is True


class TestMarkHandler:
    """Tests for the _mark_handler and _mark_external_handler functions."""

    def test_mark_handler(self):
        """Test that _mark_handler sets attribute and returns same handler."""
        handler = logging.StreamHandler()
        marked = _mark_handler(handler)
        assert getattr(marked, "_fabric_cicd_managed", False) is True
        assert marked is handler

    def test_mark_external_handler(self):
        """Test that _mark_external_handler sets attribute and returns same handler."""
        handler = logging.StreamHandler()
        marked = _mark_external_handler(handler)
        assert getattr(marked, "_fabric_cicd_external", False) is True
        assert getattr(marked, "_fabric_cicd_managed", False) is False
        assert marked is handler


class TestCleanupManagedHandlers:
    """Tests for the _cleanup_managed_handlers function."""

    def test_removes_managed_preserves_external(self):
        """Test that managed handlers are removed while external are preserved."""
        logger = logging.getLogger("test_cleanup")
        external_handler = logging.StreamHandler()
        managed_handler = _mark_handler(logging.StreamHandler())
        logger.addHandler(external_handler)
        logger.addHandler(managed_handler)

        _cleanup_managed_handlers(logger)

        assert external_handler in logger.handlers
        assert managed_handler not in logger.handlers

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

        logger_a.handlers = []
        logger_b.handlers = []

    def test_cleanup_external_handler_removes_filters(self, temp_log_dir):
        """Test cleanup removes PackageFilter from external handlers."""
        log_file = temp_log_dir / "external.log"
        external_handler = RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1)

        try:
            _mark_external_handler(external_handler)
            external_handler.addFilter(PackageFilter(debug_only=True))

            root_logger = logging.getLogger()
            root_logger.addHandler(external_handler)

            assert len(external_handler.filters) == 1
            assert isinstance(external_handler.filters[0], PackageFilter)

            _cleanup_managed_handlers(root_logger)

            assert external_handler not in root_logger.handlers
            assert len(external_handler.filters) == 0
            assert getattr(external_handler, "_fabric_cicd_external", False) is False

        finally:
            external_handler.close()


class TestConfigureDefaultFileHandler:
    """Tests for the _configure_default_file_handler function."""

    def test_default_file_handler_configuration(self):
        """Test default file handler has correct configuration."""
        handler = _configure_default_file_handler()
        try:
            assert isinstance(handler, logging.FileHandler)
            assert not isinstance(handler, RotatingFileHandler)
            assert getattr(handler, "_fabric_cicd_managed", False) is True
            assert handler.baseFilename.endswith("fabric_cicd.error.log")
            assert handler.mode == "w"
            assert handler.stream is None  # delay=True
            assert len(handler.filters) == 1
            assert isinstance(handler.filters[0], PackageFilter)
            assert handler.filters[0].debug_only is False
            assert handler.formatter is not None
        finally:
            handler.close()


class TestConfigureExternalFileHandler:
    """Tests for the _configure_external_file_handler function."""

    def test_reuses_handler_directly(self, temp_log_dir):
        """Test external file handler is reused directly (preserving rotation)."""
        log_file = temp_log_dir / "external.log"
        external_handler = RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1)
        custom_formatter = logging.Formatter("CUSTOM - %(message)s")
        external_handler.setFormatter(custom_formatter)

        try:
            handler = _configure_external_file_handler(external_handler, logging.DEBUG, debug_only_file=True)

            assert handler is external_handler
            assert isinstance(handler, RotatingFileHandler)
            assert handler.baseFilename == str(log_file)
            assert getattr(handler, "_fabric_cicd_managed", False) is False
            assert getattr(handler, "_fabric_cicd_external", False) is True
            assert handler.formatter is custom_formatter
            assert len(handler.filters) == 1
            assert isinstance(handler.filters[0], PackageFilter)
            assert handler.filters[0].debug_only is True
        finally:
            external_handler.close()

    def test_preserves_caller_formatter(self, temp_log_dir):
        """Test external file handler preserves caller's formatter."""
        log_file = temp_log_dir / "external.log"
        custom_formatter = logging.Formatter("CUSTOM - %(levelname)s - %(message)s")
        external_handler = RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1)
        external_handler.setFormatter(custom_formatter)

        try:
            handler = _configure_external_file_handler(external_handler, logging.DEBUG, debug_only_file=True)

            # Verify formatter is preserved by checking it formats correctly
            record = logging.LogRecord(
                name="fabric_cicd", level=logging.DEBUG, pathname="", lineno=0, msg="test", args=(), exc_info=None
            )
            formatted = handler.formatter.format(record)
            assert formatted.startswith("CUSTOM - DEBUG - test")
        finally:
            external_handler.close()

    def test_info_level_ignores_debug_only(self, temp_log_dir):
        """Test external file handler at INFO level ignores debug_only_file flag."""
        log_file = temp_log_dir / "external.log"
        external_handler = RotatingFileHandler(str(log_file), maxBytes=1024, backupCount=1)

        try:
            handler = _configure_external_file_handler(external_handler, logging.INFO, debug_only_file=True)
            assert handler.filters[0].debug_only is False
        finally:
            external_handler.close()

    def test_works_with_regular_file_handler(self, temp_log_dir):
        """Test external file handler works with regular FileHandler."""
        log_file = temp_log_dir / "external.log"
        external_handler = logging.FileHandler(str(log_file))

        try:
            handler = _configure_external_file_handler(external_handler, logging.DEBUG, debug_only_file=False)

            assert handler is external_handler
            assert isinstance(handler, logging.FileHandler)
            assert not isinstance(handler, RotatingFileHandler)
            assert len(handler.filters) == 1
            assert handler.filters[0].debug_only is False
        finally:
            external_handler.close()


class TestConfigureConsoleHandler:
    """Tests for the _configure_console_handler function."""

    def test_console_handler_configuration(self):
        """Test console handler has correct configuration."""
        handler = _configure_console_handler(logging.WARNING)
        assert isinstance(handler, logging.StreamHandler)
        assert handler.level == logging.WARNING
        assert getattr(handler, "_fabric_cicd_managed", False) is True
        assert isinstance(handler.formatter, CustomFormatter)


class TestGetFileHandler:
    """Tests for the get_file_handler function."""

    def test_returns_none_when_no_file_handler(self):
        """Test returns None when no file handler exists."""
        assert get_file_handler() is None

    def test_returns_managed_file_handler_from_root(self):
        """Test returns the managed file handler from root logger."""
        root_logger = logging.getLogger()
        handler = _mark_handler(logging.FileHandler("test_get.log", delay=True))
        root_logger.addHandler(handler)

        try:
            result = get_file_handler()
            assert result is handler
        finally:
            handler.close()
            root_logger.removeHandler(handler)

    def test_ignores_unmanaged_file_handler_on_root(self):
        """Test ignores file handlers not marked as managed on root logger."""
        root_logger = logging.getLogger()
        handler = logging.FileHandler("test_unmanaged.log", delay=True)
        root_logger.addHandler(handler)

        try:
            assert get_file_handler() is None
        finally:
            handler.close()
            root_logger.removeHandler(handler)

    def test_ignores_external_file_handler_on_root(self):
        """Test ignores file handlers marked as external on root logger."""
        root_logger = logging.getLogger()
        handler = _mark_external_handler(logging.FileHandler("test_external.log", delay=True))
        root_logger.addHandler(handler)

        try:
            assert get_file_handler() is None
        finally:
            handler.close()
            root_logger.removeHandler(handler)

    def test_returns_any_file_handler_from_provided_logger(self):
        """Test returns any file handler from provided logger."""
        external_logger = logging.getLogger("external_test")
        handler = logging.FileHandler("test_external.log", delay=True)
        external_logger.addHandler(handler)

        try:
            result = get_file_handler(external_logger)
            assert result is handler
        finally:
            handler.close()
            external_logger.removeHandler(handler)


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

    def test_with_non_default_file_handler(self, temp_log_dir):
        """Test message excludes file path for non-default file handlers."""
        log_file = temp_log_dir / "program.log"
        handler = logging.FileHandler(str(log_file), delay=True)

        try:
            exception = Exception("Something failed")
            result = _build_console_message(exception, handler)
            assert result == "Something failed"
            assert "See" not in result
        finally:
            handler.close()


class TestBuildFileMessage:
    """Tests for the _build_file_message function."""

    @pytest.mark.parametrize(
        ("additional_info", "expected_in_result"),
        [
            (None, False),
            ("status: 403", True),
        ],
    )
    def test_file_message(self, additional_info, expected_in_result):
        """Test file message with and without additional info."""
        exception = Exception("Something failed")
        if additional_info is not None:
            exception.additional_info = additional_info

        result = _build_file_message(exception)
        assert "%s" in result
        if expected_in_result:
            assert "Additional Info" in result
            assert additional_info in result
        else:
            assert result == "%s"


class TestConfigureLogger:
    """Tests for the configure_logger function."""

    @pytest.mark.parametrize(
        ("level", "expected_package_level", "expected_root_level"),
        [
            (logging.INFO, logging.INFO, logging.ERROR),
            (logging.DEBUG, logging.DEBUG, logging.INFO),
        ],
    )
    def test_logger_levels(self, level, expected_package_level, expected_root_level):
        """Test logger level configuration."""
        configure_logger(level=level, disable_log_file=True)

        assert logging.getLogger("fabric_cicd").level == expected_package_level
        assert logging.getLogger().level == expected_root_level

    def test_default_includes_file_handler(self):
        """Test default configuration includes file handler."""
        configure_logger()
        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1

    def test_disable_file_logging(self):
        """Test file logging can be disabled."""
        configure_logger(disable_log_file=True)
        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 0

    def test_with_external_file_handler(self, external_rotating_handler, temp_log_dir):
        """Test configuration with external file handler."""
        log_file = temp_log_dir / "external.log"

        configure_logger(
            level=logging.DEBUG,
            external_file_handler=external_rotating_handler,
            suppress_debug_console=True,
            debug_only_file=True,
        )

        root_logger = logging.getLogger()
        external_handlers = [
            h
            for h in root_logger.handlers
            if isinstance(h, logging.FileHandler) and getattr(h, "_fabric_cicd_external", False)
        ]
        assert len(external_handlers) == 1
        assert external_handlers[0] is external_rotating_handler
        assert external_handlers[0].baseFilename == str(log_file)
        assert isinstance(external_handlers[0], RotatingFileHandler)

    def test_suppress_debug_console(self):
        """Test suppressing DEBUG output to console."""
        configure_logger(level=logging.DEBUG, suppress_debug_console=True, disable_log_file=True)

        package_logger = logging.getLogger("fabric_cicd")
        console_handlers = [h for h in package_logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == logging.INFO

    def test_console_only_logger_configured(self):
        """Test console_only logger is properly configured."""
        configure_logger(disable_log_file=True)

        console_only_logger = logging.getLogger("console_only")
        package_logger = logging.getLogger("fabric_cicd")

        assert console_only_logger.propagate is False
        assert len(console_only_logger.handlers) == 1
        assert package_logger.handlers[0] is not console_only_logger.handlers[0]

    def test_preserves_unmanaged_handlers(self):
        """Test that unmanaged handlers survive reconfiguration."""
        root_logger = logging.getLogger()
        external_handler = logging.StreamHandler()
        root_logger.addHandler(external_handler)

        configure_logger(disable_log_file=True)
        configure_logger(disable_log_file=True)

        assert external_handler in root_logger.handlers
        root_logger.removeHandler(external_handler)

    def test_package_logger_propagates(self):
        """Test that package logger propagates to root."""
        configure_logger(disable_log_file=True)
        assert logging.getLogger("fabric_cicd").propagate is True


class TestLogHeader:
    """Tests for the log_header function."""

    def test_logs_expected_messages(self, caplog):
        """Test log_header logs the expected messages."""
        logger = logging.getLogger("fabric_cicd.test")
        logger.setLevel(logging.INFO)

        with caplog.at_level(logging.INFO, logger="fabric_cicd.test"):
            log_header(logger, "Test Header")

        assert len(caplog.records) >= 3
        assert any("Test Header" in record.message for record in caplog.records)


class TestWrapperFunctions:
    """Tests for the wrapper functions in __init__.py."""

    @pytest.fixture(autouse=True)
    def _clear_feature_flags(self):
        """Clear feature flags before each wrapper test."""

        constants.FEATURE_FLAG.clear()

    def test_append_feature_flag(self):
        """Test append_feature_flag adds flags correctly."""
        append_feature_flag("feature_1")
        append_feature_flag("feature_2")
        append_feature_flag("feature_1")  # Duplicate

        assert "feature_1" in constants.FEATURE_FLAG
        assert "feature_2" in constants.FEATURE_FLAG
        assert len([f for f in constants.FEATURE_FLAG if f == "feature_1"]) == 1

    @pytest.mark.parametrize("level_input", ["DEBUG", "debug"])
    def test_change_log_level(self, level_input):
        """Test change_log_level sets level correctly."""
        change_log_level(level_input)
        assert logging.getLogger("fabric_cicd").level == logging.DEBUG

    def test_change_log_level_unsupported(self, capsys):
        """Test change_log_level warns on unsupported level."""
        configure_logger(disable_log_file=True)
        change_log_level("TRACE")

        captured = capsys.readouterr()
        assert "not supported" in captured.err

    def test_disable_file_logging(self):
        """Test disable_file_logging removes file handlers."""

        configure_logger()
        disable_file_logging()

        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 0


class TestConfigureExternalFileLogging:
    """Tests for the configure_external_file_logging wrapper function."""

    def test_configures_correctly(self, external_logger_with_handler):
        """Test that configure_external_file_logging configures correctly."""
        external_logger, external_handler, log_file = external_logger_with_handler

        configure_external_file_logging(external_logger)

        package_logger = logging.getLogger("fabric_cicd")
        assert package_logger.level == logging.DEBUG

        console_handlers = [
            h
            for h in package_logger.handlers
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == logging.INFO

        root_logger = logging.getLogger()
        external_handlers = [
            h
            for h in root_logger.handlers
            if isinstance(h, logging.FileHandler) and getattr(h, "_fabric_cicd_external", False)
        ]
        assert len(external_handlers) == 1
        assert external_handlers[0] is external_handler
        assert external_handlers[0].baseFilename == str(log_file)

    def test_raises_without_handler(self):
        """Test that configure_external_file_logging raises ValueError if no file handler."""
        external_logger = logging.getLogger("NoFileHandler")
        external_logger.handlers = []

        with pytest.raises(ValueError, match="No FileHandler or RotatingFileHandler found"):
            configure_external_file_logging(external_logger)

    def test_writes_only_debug_logs(self, external_logger_with_handler):
        """Test that only DEBUG logs from fabric_cicd are written to external file."""
        external_logger, _external_handler, log_file = external_logger_with_handler

        configure_external_file_logging(external_logger)

        fabric_logger = logging.getLogger("fabric_cicd")
        fabric_logger.debug("Debug message")
        fabric_logger.info("Info message")

        azure_logger = logging.getLogger("azure.identity")
        azure_logger.setLevel(logging.DEBUG)
        azure_logger.debug("Azure debug")

        for handler in logging.getLogger().handlers:
            if hasattr(handler, "flush"):
                handler.flush()

        content = log_file.read_text(encoding="utf-8")
        assert "Debug message" in content
        assert "Info message" not in content
        assert "Azure debug" not in content


class TestExceptionHandler:
    """Tests for the exception_handler function."""

    def test_handles_custom_exception(self):
        """Test exception handler handles custom exceptions."""
        from fabric_cicd._common._exceptions import InputError

        test_logger = logging.getLogger("fabric_cicd.test")
        exception = InputError("Test error message", logger=test_logger)
        configure_logger(disable_log_file=True)

        try:
            exception_handler(InputError, exception, None)
        except Exception:
            pytest.fail("exception_handler raised an unexpected exception")

    def test_falls_back_for_standard_exception(self):
        """Test exception handler falls back to default for standard exceptions."""
        with patch("sys.__excepthook__") as mock_excepthook:
            exception = ValueError("Standard error")
            exception_handler(ValueError, exception, None)
            mock_excepthook.assert_called_once()

    def test_writes_to_console_only_logger(self):
        """Test that exception handler writes to console_only logger."""
        from fabric_cicd._common._exceptions import InputError

        configure_logger(disable_log_file=True)
        test_logger = logging.getLogger("fabric_cicd.test")
        exception = InputError("User-facing error", logger=test_logger)

        with patch.object(logging.getLogger("console_only"), "error") as mock_error:
            exception_handler(InputError, exception, None)
            mock_error.assert_called_once()
            message = mock_error.call_args[0][0]
            assert "User-facing error" in message
            assert "See" not in message

    def test_removes_console_handler_when_using_default_file(self):
        """Test that exception handler removes console handler when using default file handler."""
        from fabric_cicd._common._exceptions import InputError

        configure_logger()
        test_logger = logging.getLogger("fabric_cicd.test")
        exception = InputError("Test error", logger=test_logger)

        package_logger = logging.getLogger("fabric_cicd")
        assert len(package_logger.handlers) >= 1

        exception_handler(InputError, exception, None)

        managed_handlers = [h for h in package_logger.handlers if getattr(h, "_fabric_cicd_managed", False)]
        assert len(managed_handlers) == 0


class TestFileLoggingIntegration:
    """Integration tests for file logging functionality."""

    def test_default_file_handler_writes_logs(self, temp_log_dir):
        """Test that default file handler actually writes logs."""
        import os

        original_cwd = Path.cwd()

        try:
            os.chdir(temp_log_dir)
            configure_logger()

            logger = logging.getLogger("fabric_cicd")
            logger.error("Error message for test")

            for handler in logging.getLogger().handlers:
                if hasattr(handler, "flush"):
                    handler.flush()

            log_file = temp_log_dir / "fabric_cicd.error.log"
            assert log_file.exists()
            content = log_file.read_text(encoding="utf-8")
            assert "Error message for test" in content

        finally:
            os.chdir(original_cwd)

    def test_file_not_created_until_log_written(self, temp_log_dir):
        """Test that log file is not created until first log is written (delay=True)."""
        import os

        original_cwd = Path.cwd()

        try:
            os.chdir(temp_log_dir)
            configure_logger()

            log_file = temp_log_dir / "fabric_cicd.error.log"
            assert not log_file.exists()

        finally:
            os.chdir(original_cwd)

    def test_external_handler_writes_fabric_cicd_logs(self, external_logger_with_handler):
        """Test that external handler writes fabric_cicd logs to the shared file."""
        external_logger, external_handler, log_file = external_logger_with_handler

        external_logger.debug("Program message 1")

        configure_external_file_logging(external_logger)

        fabric_logger = logging.getLogger("fabric_cicd")
        fabric_logger.debug("Fabric CICD message")

        for handler in logging.getLogger().handlers:
            if hasattr(handler, "flush"):
                handler.flush()
        external_handler.flush()

        content = log_file.read_text(encoding="utf-8")
        assert "Program message 1" in content
        assert "Fabric CICD message" in content

    def test_console_only_logger_does_not_propagate_to_file(self, external_logger_with_handler):
        """Test that console_only logger does not write to file."""
        external_logger, _external_handler, log_file = external_logger_with_handler

        configure_external_file_logging(external_logger)

        console_only_logger = logging.getLogger("console_only")
        console_only_logger.error("Console only error")

        for handler in logging.getLogger().handlers:
            if hasattr(handler, "flush"):
                handler.flush()

        if log_file.exists():
            content = log_file.read_text(encoding="utf-8")
            assert "Console only error" not in content


class TestExternalHandlerReconfiguration:
    """Tests for external handler reconfiguration scenarios."""

    def test_debug_to_non_debug_cleans_up_filter(self, external_logger_with_handler):
        """Test switching from debug mode to non-debug mode cleans up filter."""
        external_logger, external_handler, _log_file = external_logger_with_handler

        configure_external_file_logging(external_logger)
        assert len(external_handler.filters) == 1
        assert isinstance(external_handler.filters[0], PackageFilter)

        disable_file_logging()

        assert len(external_handler.filters) == 0
        assert getattr(external_handler, "_fabric_cicd_external", False) is False

    def test_non_debug_to_debug_adds_filter(self, external_logger_with_handler):
        """Test switching from non-debug mode to debug mode adds filter correctly."""
        external_logger, external_handler, _log_file = external_logger_with_handler

        disable_file_logging()
        assert len(external_handler.filters) == 0

        configure_external_file_logging(external_logger)

        assert len(external_handler.filters) == 1
        assert isinstance(external_handler.filters[0], PackageFilter)
        assert external_handler.filters[0].debug_only is True

    def test_multiple_debug_runs_no_filter_accumulation(self, external_logger_with_handler):
        """Test multiple debug runs don't accumulate filters on the same handler."""
        external_logger, external_handler, _log_file = external_logger_with_handler

        configure_external_file_logging(external_logger)
        configure_external_file_logging(external_logger)
        configure_external_file_logging(external_logger)

        assert len(external_handler.filters) == 1
        assert isinstance(external_handler.filters[0], PackageFilter)

    def test_handler_not_closed_on_disable(self, external_logger_with_handler):
        """Test external handler is not closed when file logging is disabled."""
        external_logger, external_handler, log_file = external_logger_with_handler

        configure_external_file_logging(external_logger)
        disable_file_logging()

        external_logger.debug("Message after disable")
        external_handler.flush()

        content = log_file.read_text(encoding="utf-8")
        assert "Message after disable" in content

    def test_rotating_handler_preserves_rotation_settings(self, external_logger_with_handler):
        """Test that RotatingFileHandler rotation settings are preserved."""
        external_logger, external_handler, _log_file = external_logger_with_handler

        original_max_bytes = external_handler.maxBytes
        original_backup_count = external_handler.backupCount

        configure_external_file_logging(external_logger)

        root_logger = logging.getLogger()
        external_handlers = [
            h
            for h in root_logger.handlers
            if isinstance(h, RotatingFileHandler) and getattr(h, "_fabric_cicd_external", False)
        ]

        assert len(external_handlers) == 1
        handler = external_handlers[0]
        assert handler.maxBytes == original_max_bytes
        assert handler.backupCount == original_backup_count
