# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
import json
import logging
from unittest.mock import Mock

import pytest

from fabric_cicd._common._http_tracer import FileTracer, HTTPTracerFactory, NoOpTracer

# --- _validate_output_path ---


def test_validate_output_path_valid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = FileTracer._validate_output_path("trace.json")
    assert result == str((tmp_path / "trace.json").resolve())


def test_validate_output_path_subdirectory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sub = tmp_path / "subdir"
    sub.mkdir()
    result = FileTracer._validate_output_path("subdir/trace.json")
    assert result == str((sub / "trace.json").resolve())


def test_validate_output_path_rejects_non_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match=r"must have a \.json extension"):
        FileTracer._validate_output_path("trace.txt")


def test_validate_output_path_rejects_path_traversal(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="must be within the current working directory"):
        FileTracer._validate_output_path("../../evil.json")


def test_validate_output_path_rejects_sibling_directory_prefix(tmp_path, monkeypatch):
    """Ensure a sibling directory whose name starts with the CWD name is rejected."""
    project_dir = tmp_path / "myproject"
    project_dir.mkdir()
    monkeypatch.chdir(project_dir)
    sibling = tmp_path / "myproject-evil"
    sibling.mkdir()
    evil_path = str(sibling / "trace.json")
    with pytest.raises(ValueError, match="must be within the current working directory"):
        FileTracer._validate_output_path(evil_path)


def test_validate_output_path_rejects_embedded_null_byte(tmp_path, monkeypatch):
    """Ensure an unresolvable path raises ValueError with context."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="Failed to resolve HTTP trace file path"):
        FileTracer._validate_output_path("\x00invalid.json")


# --- FileTracer warning log ---


def test_file_tracer_emits_warning(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)
    with caplog.at_level(logging.WARNING):
        FileTracer(output_file="trace.json")
    assert "HTTP tracing is enabled" in caplog.text
    assert "Do not commit or share this file" in caplog.text


# --- capture_request filters sensitive headers ---


def test_capture_request_filters_authorization(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    tracer = FileTracer(output_file="trace.json")
    headers = {
        "Authorization": "Bearer secret-token",
        "Content-Type": "application/json",
        "User-Agent": "test-agent",
    }
    tracer.capture_request("GET", "https://api.example.com/test", headers, "{}", None)

    assert len(tracer.captures) == 1
    request_data = json.loads(base64.b64decode(tracer.captures[0]["request_b64"]).decode())
    assert "Authorization" not in request_data["headers"]
    assert "authorization" not in request_data["headers"]
    assert request_data["headers"]["Content-Type"] == "application/json"
    assert request_data["headers"]["User-Agent"] == "test-agent"


# --- capture_response filters sensitive headers ---


def test_capture_response_filters_sensitive_headers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    tracer = FileTracer(output_file="trace.json")
    tracer.captures.append({"request_b64": "", "response_b64": None})

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {
        "Content-Type": "application/json",
        "Set-Cookie": "session=abc123",
        "WWW-Authenticate": "Bearer realm=test",
        "Proxy-Authenticate": "Basic",
        "x-ms-aad-diagnostic-headers": "diag-data",
        "X-Request-Id": "req-123",
    }
    mock_response.json.return_value = {"result": "ok"}

    tracer.capture_response(mock_response)

    response_data = json.loads(base64.b64decode(tracer.captures[0]["response_b64"]).decode())
    assert "Set-Cookie" not in response_data["headers"]
    assert "set-cookie" not in response_data["headers"]
    assert "WWW-Authenticate" not in response_data["headers"]
    assert "Proxy-Authenticate" not in response_data["headers"]
    assert "x-ms-aad-diagnostic-headers" not in response_data["headers"]
    assert response_data["headers"]["Content-Type"] == "application/json"
    assert response_data["headers"]["X-Request-Id"] == "req-123"


def test_capture_response_no_captures_is_noop(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    tracer = FileTracer(output_file="trace.json")
    mock_response = Mock()
    tracer.capture_response(mock_response)
    assert len(tracer.captures) == 0


# --- HTTPTracerFactory ---


def test_factory_creates_file_tracer_when_enabled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FABRIC_CICD_HTTP_TRACE_ENABLED", "1")
    tracer = HTTPTracerFactory.create()
    assert isinstance(tracer, FileTracer)


def test_factory_creates_noop_tracer_when_disabled(monkeypatch):
    monkeypatch.delenv("FABRIC_CICD_HTTP_TRACE_ENABLED", raising=False)
    tracer = HTTPTracerFactory.create()
    assert isinstance(tracer, NoOpTracer)


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "yes", "Yes", "1"])
def test_factory_accepts_valid_enable_flags(tmp_path, monkeypatch, value):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FABRIC_CICD_HTTP_TRACE_ENABLED", value)
    tracer = HTTPTracerFactory.create()
    assert isinstance(tracer, FileTracer)


@pytest.mark.parametrize("value", ["0", "false", "no", ""])
def test_factory_rejects_invalid_enable_flags(monkeypatch, value):
    monkeypatch.setenv("FABRIC_CICD_HTTP_TRACE_ENABLED", value)
    tracer = HTTPTracerFactory.create()
    assert isinstance(tracer, NoOpTracer)


# --- FileTracer uses env var for output path ---


def test_file_tracer_uses_env_var_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FABRIC_CICD_HTTP_TRACE_FILE", "custom_trace.json")
    tracer = FileTracer()
    assert tracer.output_file == str((tmp_path / "custom_trace.json").resolve())


def test_file_tracer_defaults_to_http_trace_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FABRIC_CICD_HTTP_TRACE_FILE", raising=False)
    tracer = FileTracer()
    assert tracer.output_file == str((tmp_path / "http_trace.json").resolve())
