# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""HTTP request/response tracer for debugging and mock server generation."""

import base64
import csv
import hashlib
import json
import logging
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Optional, Protocol
from urllib.parse import urlparse

import requests

from fabric_cicd.constants import AUTHORIZATION_HEADER, EnvVar

logger = logging.getLogger(__name__)


class RouteNormalizer:
    """Normalizes workspace IDs in URLs for consistent mocking across different workspaces.

    Attributes:
        NORMALIZATION_RULES: List of (pattern, replacement) tuples for URL normalization.
            Each pattern should have two capture groups:
            1. The prefix (e.g., '/workspaces/')
            2. The GUID to replace
            The replacement value will be substituted for the GUID while preserving the prefix.
    """

    NORMALIZATION_RULES: ClassVar[list[tuple[re.Pattern, str]]] = [
        (
            re.compile(r"(/workspaces/)([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})"),
            "00000000-0000-0000-0000-000000000000",
        ),
    ]

    @classmethod
    def normalize(cls, url: str) -> str:
        """
        Normalize workspace IDs in URLs to a constant value.

        Args:
            url: URL that may contain workspace GUIDs

        Returns:
            URL with workspace GUIDs replaced by normalized ID
        """
        normalized_url = url
        for pattern, replacement in cls.NORMALIZATION_RULES:
            normalized_url = pattern.sub(lambda match, repl=replacement: match.group(1) + repl, normalized_url)

        return normalized_url


@dataclass
class HTTPRequest:
    """Represents an HTTP request with metadata."""

    method: str
    url: str
    headers: dict[str, str]
    body: Any
    timestamp: str

    def to_b64(self) -> str:
        """Serialize to base64-encoded JSON."""
        request_json = json.dumps(asdict(self), separators=(",", ":"))
        return base64.b64encode(request_json.encode()).decode()

    @classmethod
    def from_b64(cls, b64_str: str) -> "HTTPRequest":
        """Deserialize from base64-encoded JSON."""
        json_str = base64.b64decode(b64_str).decode()
        data = json.loads(json_str)
        return cls(**data)

    def get_unique_signature(self) -> str:
        """Generate unique signature from URL, method, and body using SHA256."""
        body_str = json.dumps(self.body, sort_keys=True) if isinstance(self.body, dict) else str(self.body or "")
        return hashlib.sha256(f"{self.url}{self.method}{body_str}".encode()).hexdigest()

    def get_route_key(self) -> str:
        """Extract route key (method + path + query) from the request."""
        try:
            parsed_url = urlparse(self.url)
            route = parsed_url.path
            if parsed_url.query:
                route += f"?{parsed_url.query}"
            return f"{self.method} {route}"
        except Exception:
            return ""


@dataclass
class HTTPResponse:
    """Represents an HTTP response with metadata."""

    status_code: int
    headers: dict[str, str]
    body: Any
    timestamp: str

    def to_b64(self) -> str:
        """Serialize to base64-encoded JSON."""
        response_json = json.dumps(asdict(self), separators=(",", ":"))
        return base64.b64encode(response_json.encode()).decode()

    @classmethod
    def from_b64(cls, b64_str: str) -> "HTTPResponse":
        """Deserialize from base64-encoded JSON."""
        json_str = base64.b64decode(b64_str).decode()
        data = json.loads(json_str)
        return cls(**data)

    def get_unique_signature(self) -> str:
        """Generate unique signature from status code and body using SHA256."""
        body_str = json.dumps(self.body, sort_keys=True) if isinstance(self.body, dict) else str(self.body or "")
        return hashlib.sha256(f"{self.status_code}{body_str}".encode()).hexdigest()


class HTTPTracer(Protocol):
    """Protocol for HTTP request/response tracers."""

    def capture_request(self, method: str, url: str, headers: dict, body: str, files: Optional[dict]) -> None:
        """Capture HTTP request details."""
        ...

    def capture_response(self, response: requests.Response) -> None:
        """Capture HTTP response details."""
        ...

    def save(self) -> None:
        """Save captured data."""
        ...


class NoOpTracer:
    """No-op tracer that does nothing."""

    def capture_request(self, method: str, url: str, headers: dict, body: str, files: Optional[dict]) -> None:
        """No-op capture request."""
        pass

    def capture_response(self, response: requests.Response) -> None:
        """No-op capture response."""
        pass

    def save(self) -> None:
        """No-op save."""
        pass


class FileTracer:
    """Captures HTTP requests and responses to a CSV file."""

    def __init__(self, output_file: Optional[str] = None) -> None:
        """
        Initialize the file tracer.

        Args:
            output_file: Path to save the trace file. If None, checks EnvVar.HTTP_TRACE_FILE.
        """
        trace_file_from_env = os.environ.get(EnvVar.HTTP_TRACE_FILE.value)

        if output_file is None:
            self.output_file = trace_file_from_env if trace_file_from_env else "http_trace.csv"
        else:
            self.output_file = output_file

        self.captures = []

    def capture_request(self, method: str, url: str, headers: dict, body: str, files: Optional[dict]) -> None:  # noqa: ARG002
        """
        Capture HTTP request details with base64 encoding, if enabled.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Request headers. Note: Authorization headers are excluded.
            body: Request body
            files: Files being uploaded
        """
        request = HTTPRequest(
            method=method,
            url=RouteNormalizer.normalize(url),
            headers={k: v for k, v in headers.items() if k.lower() not in [AUTHORIZATION_HEADER]},
            body=body,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        self.captures.append({"request_b64": request.to_b64(), "response_b64": None})

    def capture_response(self, response: requests.Response) -> None:
        """
        Add response data to the most recent capture entry.

        Args:
            response: The HTTP response object
        """
        if not self.captures:
            return

        try:
            if hasattr(response, "json") and "application/json" in response.headers.get("Content-Type", ""):
                response_body = response.json()
            else:
                response_body = response.text if hasattr(response, "text") else ""
        except Exception:
            response_body = ""

        http_response = HTTPResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            body=response_body,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        self.captures[-1]["response_b64"] = http_response.to_b64()

    def save(self) -> None:
        """Save all HTTP captures to a CSV file."""
        if not self.captures:
            return

        try:
            output_path = Path(self.output_file)
            file_exists = output_path.exists()

            with output_path.open("a" if file_exists else "w", newline="") as f:
                writer = csv.writer(f)

                if not file_exists:
                    writer.writerow(["request_b64", "response_b64"])

                for capture in self.captures:
                    request_b64 = capture.get("request_b64", "")
                    response_b64 = capture.get("response_b64", "")
                    writer.writerow([request_b64, response_b64])
        except Exception as e:
            logger.warning(f"Failed to save HTTP trace: {e}")


class HTTPTracerFactory:
    """Factory class for creating HTTP tracer instances."""

    @staticmethod
    def create() -> HTTPTracer:
        """
        Create an HTTP tracer based on environment configuration.

        Returns:
            FileTracer if tracing is enabled via environment variable, NoOpTracer otherwise.
        """
        from fabric_cicd.constants import VALID_ENABLE_FLAGS

        trace_enabled = os.environ.get(EnvVar.HTTP_TRACE_ENABLED.value, "").lower() in VALID_ENABLE_FLAGS
        return FileTracer() if trace_enabled else NoOpTracer()
