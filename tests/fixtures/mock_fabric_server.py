# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Stateful Mock Fabric REST API server for integration testing.

Provides transactionally correct responses via:
- Content-based matching for POST/PATCH (by displayName + type)
- Operation ID correlation for async 202 responses
- State machine for long-running operations (Running -> Succeeded)
- Generic fallback responses for unknown mutation routes
"""

import json
import logging
import re
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, ClassVar, Optional
from urllib.parse import urlparse

from fabric_cicd._common._http_tracer import HTTPRequest, HTTPResponse

logger = logging.getLogger(__name__)

MOCK_SERVER_PORT = 8765
GUID_PATTERN = re.compile(r"[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")
OPERATION_PATTERN = re.compile(r"/v1/operations/([a-fA-F0-9-]+)(/result)?$")
SKIP_HEADERS = {
    "content-length",
    "content-encoding",
    "transfer-encoding",
    "server",
    "date",
    "home-cluster-uri",
    "request-redirected",
}


class TraceIndex:
    """
    Multi-index for trace lookup: by normalized route, by content (displayName, type),
    and by operation ID for async operation correlation.
    """

    def __init__(self):
        self.by_route: dict[str, list[tuple[HTTPRequest, HTTPResponse]]] = defaultdict(list)
        self.by_content: dict[tuple[str, str, str], list[tuple[HTTPRequest, HTTPResponse]]] = defaultdict(list)
        self.by_operation: dict[str, dict[str, Any]] = defaultdict(lambda: {"post": None, "poll": [], "result": None})

    @staticmethod
    def normalize_route(route: str) -> str:
        """Replace all GUIDs with {GUID} placeholder."""
        return GUID_PATTERN.sub("{GUID}", route)

    @staticmethod
    def extract_content_key(body: Any, method: str) -> Optional[tuple[str, str, str]]:
        """Extract (displayName, type, method) from request body if present."""
        if isinstance(body, dict) and body.get("displayName") and body.get("type"):
            return (body["displayName"], body["type"], method)
        return None

    def add_trace(self, request: HTTPRequest, response: HTTPResponse):
        """Index a trace by route, content, and operation ID."""
        parsed = urlparse(request.url)
        route = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        normalized_key = f"{request.method} {self.normalize_route(route)}"

        self.by_route[normalized_key].append((request, response))

        if (
            request.method in ("POST", "PATCH")
            and request.body
            and (content_key := self.extract_content_key(request.body, request.method))
        ):
            self.by_content[content_key].append((request, response))

        if (op_id := response.headers.get("x-ms-operation-id")) and response.status_code == 202:
            self.by_operation[op_id]["post"] = (request, response)

        if op_match := OPERATION_PATTERN.search(route):
            op_id, is_result = op_match.group(1), op_match.group(2) == "/result"
            if is_result:
                self.by_operation[op_id]["result"] = (request, response)
            else:
                self.by_operation[op_id]["poll"].append((request, response))


class MockFabricAPIHandler(BaseHTTPRequestHandler):
    """HTTP handler with stateful matching: content-based for items, state machine for operations."""

    trace_index: ClassVar[TraceIndex] = TraceIndex()
    route_lock: ClassVar[threading.Lock] = threading.Lock()
    operation_poll_counts: ClassVar[dict[str, int]] = {}
    content_to_operation: ClassVar[dict[tuple[str, str], str]] = {}

    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_GET(self):  # noqa: N802
        self._handle_request("GET")

    def do_POST(self):  # noqa: N802
        self._handle_request("POST")

    def do_PATCH(self):  # noqa: N802
        self._handle_request("PATCH")

    def do_DELETE(self):  # noqa: N802
        self._handle_request("DELETE")

    def _read_request_body(self) -> Optional[dict]:
        """Read and parse JSON request body."""
        if content_length := self.headers.get("Content-Length"):
            try:
                return json.loads(self.rfile.read(int(content_length)).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        return None

    def _handle_request(self, method: str):
        """Route request to appropriate handler, with fallback for unknown routes."""
        route_key = f"{method} {self.path}"
        logger.info(f"Mock server received: {route_key}")

        request_body = self._read_request_body() if method in ("POST", "PATCH") else None
        response = self._find_matching_response(method, self.path, request_body)

        if response is None:
            if method in ("PATCH", "DELETE"):
                logger.info(f"No trace for {route_key}, returning 200 OK")
                response = HTTPResponse(
                    status_code=200, headers={"Content-Type": "application/json"}, body={}, timestamp=None
                )
            elif method == "POST":
                logger.info(f"No trace for {route_key}, returning 202 Accepted")
                response = HTTPResponse(
                    status_code=202, headers={"Content-Type": "application/json"}, body={}, timestamp=None
                )
            else:
                logger.warning(f"No trace data for {route_key}")
                self.send_error(404, f"No trace data found for {route_key}")
                return

        self._send_response(response, route_key)

    def _find_matching_response(self, method: str, route: str, request_body: Optional[dict]) -> Optional[HTTPResponse]:
        """Find response by: 1) operation polling, 2) content match, 3) route match."""
        normalized_key = f"{method} {self.trace_index.normalize_route(route)}"

        if op_match := OPERATION_PATTERN.search(route):
            return self._handle_operation_request(op_match.group(1), op_match.group(2) == "/result")

        if (
            method == "POST"
            and route.endswith("/items")
            and request_body
            and (content_key := self.trace_index.extract_content_key(request_body, method))
        ):
            return self._handle_item_creation(content_key)

        if (
            method == "POST"
            and "updateDefinition" in route
            and (traces := self.trace_index.by_route.get(normalized_key))
        ):
            return traces[-1][1]

        if traces := self.trace_index.by_route.get(normalized_key):
            return traces[-1][1]

        return None

    def _handle_item_creation(self, content_key: tuple[str, str, str]) -> Optional[HTTPResponse]:
        """Match POST /items by (displayName, type), track operation for async responses."""
        display_name, item_type, _ = content_key
        traces = self.trace_index.by_content.get(content_key, [])

        if not traces:
            logger.warning(f"No trace for item creation: {display_name} ({item_type})")
            return None

        for _, response in traces:
            if response.status_code in (200, 201, 202):
                if response.status_code == 202 and (op_id := response.headers.get("x-ms-operation-id")):
                    with self.route_lock:
                        self.content_to_operation[(display_name, item_type)] = op_id
                        self.operation_poll_counts[op_id] = 0
                    logger.info(f"Tracking operation {op_id} for {display_name} ({item_type})")
                logger.info(f"Matched item creation: {display_name} ({item_type}) -> {response.status_code}")
                return response

        return traces[-1][1]

    def _handle_operation_request(self, operation_id: str, is_result: bool) -> Optional[HTTPResponse]:
        """
        State machine for operation polling: first poll returns Running, subsequent return Succeeded.
        Result endpoint returns the created item details.
        """
        op_data = self.trace_index.by_operation.get(operation_id)
        if not op_data:
            for _, known_data in self.trace_index.by_operation.items():
                if known_data["post"] or known_data["result"]:
                    op_data = known_data
                    break

        if not op_data:
            logger.warning(f"No operation data for {operation_id}")
            return None

        if is_result:
            if op_data["result"]:
                logger.info(f"Returning operation result for {operation_id}")
                return op_data["result"][1]
            return None

        with self.route_lock:
            poll_count = self.operation_poll_counts.get(operation_id, 0)
            self.operation_poll_counts[operation_id] = poll_count + 1

        poll_traces = op_data.get("poll", [])
        if not poll_traces:
            logger.warning(f"No poll traces for operation {operation_id}")
            return None

        target_status = "Running" if poll_count == 0 else "Succeeded"
        for _, response in poll_traces:
            if isinstance(response.body, dict) and response.body.get("status") == target_status:
                logger.info(f"Operation {operation_id} poll #{poll_count}: {target_status}")
                return response

        return poll_traces[-1][1]

    def _send_response(self, response: HTTPResponse, route_key: str):
        """Send HTTP response, rewriting Location headers for operations."""
        self.send_response(response.status_code)

        body_bytes = json.dumps(response.body if isinstance(response.body, dict) else {}).encode()
        if isinstance(response.body, str) and response.body:
            body_bytes = response.body.encode()

        for name, value in response.headers.items():
            lower = name.lower()
            if lower in ("x-ms-operation-id", "retry-after"):
                self.send_header(name, value)
            elif lower == "location" and "operations" in value and (op_match := OPERATION_PATTERN.search(value)):
                suffix = "/result" if op_match.group(2) == "/result" else ""
                self.send_header(
                    "Location", f"http://127.0.0.1:{MOCK_SERVER_PORT}/v1/operations/{op_match.group(1)}{suffix}"
                )
            elif lower not in SKIP_HEADERS:
                self.send_header(name, value)

        self.send_header("Content-Length", len(body_bytes))
        self.end_headers()
        self.wfile.write(body_bytes)
        logger.debug(f"Responded to {route_key}: {response.status_code}")

    @classmethod
    def load_trace_data(cls, trace_file: Path):
        """Load trace data from JSON file and build indices."""
        cls.trace_index = TraceIndex()
        cls.operation_poll_counts.clear()
        cls.content_to_operation.clear()

        with trace_file.open("r") as f:
            data = json.load(f)

        loaded = 0
        for trace in data.get("traces", []):
            try:
                req, resp = trace.get("request"), trace.get("response")
                if not req or not resp:
                    continue
                cls.trace_index.add_trace(
                    HTTPRequest(
                        req.get("method", ""),
                        req.get("url", ""),
                        req.get("headers", {}),
                        req.get("body"),
                        req.get("timestamp"),
                    ),
                    HTTPResponse(
                        resp.get("status_code", 200), resp.get("headers", {}), resp.get("body"), resp.get("timestamp")
                    ),
                )
                loaded += 1
            except Exception as e:
                logger.warning(f"Failed to parse trace: {e}")

        logger.info(
            f"Loaded {loaded} traces (routes={len(cls.trace_index.by_route)}, content={len(cls.trace_index.by_content)}, ops={len(cls.trace_index.by_operation)})"
        )


class MockFabricServer:
    """Mock Fabric API server for testing."""

    HTTP_TRACE_FILE = "http_trace.json.gz"

    def __init__(self, trace_file: Path, port: int = MOCK_SERVER_PORT):
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
