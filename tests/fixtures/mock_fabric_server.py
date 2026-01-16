# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Mock Fabric REST API server for integration testing."""

import json
import logging
import re
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import ClassVar, Optional
from urllib.parse import urlparse

from fabric_cicd._common._http_tracer import HTTPRequest, HTTPResponse

logger = logging.getLogger(__name__)

MOCK_SERVER_PORT = 8765
GUID_PATTERN = re.compile(r"[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")


class MockFabricAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler that serves mocked Fabric API responses."""

    trace_data: ClassVar[dict[str, list[tuple[HTTPRequest, HTTPResponse]]]] = {}
    route_invocations: ClassVar[dict[str, int]] = defaultdict(int)
    route_lock: ClassVar[threading.Lock] = threading.Lock()

    @staticmethod
    def normalize_route(route: str) -> str:
        """Replace all GUIDs in route with placeholder for pattern matching."""
        return GUID_PATTERN.sub("{GUID}", route)

    def log_message(self, format, *args):  # noqa: A002
        """Suppress default HTTP server logging."""
        pass

    def do_GET(self):  # noqa: N802
        """Handle GET requests."""
        self._handle_request("GET")

    def do_POST(self):  # noqa: N802
        """Handle POST requests."""
        self._handle_request("POST")

    def do_PATCH(self):  # noqa: N802
        """Handle PATCH requests."""
        self._handle_request("PATCH")

    def do_DELETE(self):  # noqa: N802
        """Handle DELETE requests."""
        self._handle_request("DELETE")

    def _handle_request(self, method: str):
        """
        Handle HTTP request by finding matching trace response.

        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
        """
        route = self.path
        route_key = f"{method} {route}"

        with self.route_lock:
            invocation_count = self.route_invocations[route_key]
            self.route_invocations[route_key] += 1

        logger.info(f"Mock server received: {route_key} (invocation #{invocation_count})")

        matched_route_key = None
        if route_key in self.trace_data:
            matched_route_key = route_key
        else:
            normalized_route = self.normalize_route(route_key)
            logger.info(f"Exact match not found, trying pattern: {normalized_route}")

            for known_route_key in self.trace_data:
                if self.normalize_route(known_route_key) == normalized_route:
                    matched_route_key = known_route_key
                    logger.info(f"Matched pattern to: {known_route_key}")
                    break

        if not matched_route_key:
            logger.warning(f"No trace data found for {route_key}")
            self.send_error(404, f"No trace data found for {route_key}")
            return

        responses = self.trace_data[matched_route_key]

        if invocation_count == 0 and len(responses) > 1:
            _, response = responses[0]
        else:
            _, response = responses[-1]

        self.send_response(response.status_code)

        if isinstance(response.body, dict):
            import json

            body_bytes = json.dumps(response.body).encode()
        elif isinstance(response.body, str) and response.body:
            body_bytes = response.body.encode()
        else:
            import json

            body_bytes = json.dumps({}).encode()

        for header_name, header_value in response.headers.items():
            if header_name.lower() not in [
                "content-length",
                "content-encoding",
                "server",
                "date",
                "home-cluster-uri",
                "request-redirected",
                "location",
            ]:
                self.send_header(header_name, header_value)

        self.send_header("Content-Length", len(body_bytes))
        self.end_headers()
        self.wfile.write(body_bytes)

        logger.debug(f"Mock server request for {route_key}: method={method}, path={self.path}")
        logger.debug(
            f"Mock server responding to {route_key}: status={response.status_code}, body_length={len(body_bytes)}"
        )
        try:
            body_json = json.loads(body_bytes.decode("utf-8"))
            formatted_body = json.dumps(body_json, indent=2)
        except (json.JSONDecodeError, UnicodeDecodeError):
            formatted_body = body_bytes.decode("utf-8", errors="replace")
        logger.debug(f"Mock server response body for {route_key}:\n{formatted_body}")

    @classmethod
    def load_trace_data(cls, trace_file: Path):
        """
        Load trace data from JSON file.

        Args:
            trace_file: Path to the http_trace.json file
        """
        cls.trace_data.clear()
        cls.route_invocations.clear()

        with trace_file.open("r") as f:
            data = json.load(f)

        traces = data.get("traces", [])
        for trace in traces:
            try:
                request_data = trace.get("request")
                response_data = trace.get("response")

                if not request_data or not response_data:
                    continue

                request = HTTPRequest(
                    method=request_data.get("method", ""),
                    url=request_data.get("url", ""),
                    headers=request_data.get("headers", {}),
                    body=request_data.get("body"),
                    timestamp=request_data.get("timestamp"),
                )

                response = HTTPResponse(
                    status_code=response_data.get("status_code", 200),
                    headers=response_data.get("headers", {}),
                    body=response_data.get("body"),
                    timestamp=response_data.get("timestamp"),
                )

                parsed_url = urlparse(request.url)
                route = parsed_url.path
                if parsed_url.query:
                    route += f"?{parsed_url.query}"

                route_key = f"{request.method} {route}"

                if route_key not in cls.trace_data:
                    cls.trace_data[route_key] = []

                cls.trace_data[route_key].append((request, response))
            except Exception as e:
                logger.warning(f"Failed to parse trace entry: {e}")
                continue


class MockFabricServer:
    """Mock Fabric API server for testing."""

    def __init__(self, trace_file: Path, port: int = MOCK_SERVER_PORT):
        """
        Initialize mock server.

        Args:
            trace_file: Path to http_trace.json
            port: Port to listen on
        """
        self.port = port
        self.trace_file = trace_file
        self.server: Optional[HTTPServer] = None
        self.server_thread: Optional[threading.Thread] = None

    def start(self):
        """Start the mock server in a background thread."""
        MockFabricAPIHandler.load_trace_data(self.trace_file)

        self.server = HTTPServer(("127.0.0.1", self.port), MockFabricAPIHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

        logger.info(f"Mock Fabric API server started on http://127.0.0.1:{self.port}")

    def stop(self):
        """Stop the mock server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()

        if self.server_thread:
            self.server_thread.join(timeout=5)

        logger.info("Mock Fabric API server stopped")
