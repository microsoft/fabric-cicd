# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""

Stateful Mock Fabric REST API server for integration testing.

This mock server provides transactionally correct responses by:
1. Content-based matching for POST/PATCH requests (matching by displayName + type)
2. Operation ID correlation for async operations (202 responses)
3. State machine for long-running operations (Running -> Succeeded)
4. Dynamic item state tracking across GET/POST/PATCH/DELETE

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


class TraceIndex:
    """
    Index for efficient trace lookup with content-based matching.

    Organizes traces by:
    - Route pattern (with GUIDs normalized)
    - Request content (displayName + type for POST /items)
    - Operation ID (for async operation correlation)
    """

    def __init__(self):
        # route_pattern -> list of (request, response) tuples
        self.by_route: dict[str, list[tuple[HTTPRequest, HTTPResponse]]] = defaultdict(list)

        # (displayName, type, method) -> list of (request, response) for content matching
        self.by_content: dict[tuple[str, str, str], list[tuple[HTTPRequest, HTTPResponse]]] = defaultdict(list)

        # operation_id -> {"post": (req, resp), "poll": [(req, resp)], "result": (req, resp)}
        self.by_operation: dict[str, dict[str, Any]] = defaultdict(lambda: {"post": None, "poll": [], "result": None})

        # item_id -> item details from traces
        self.items: dict[str, dict] = {}

    @staticmethod
    def normalize_route(route: str) -> str:
        """Replace all GUIDs in route with placeholder for pattern matching."""
        return GUID_PATTERN.sub("{GUID}", route)

    @staticmethod
    def extract_content_key(request_body: Any, method: str) -> Optional[tuple[str, str, str]]:
        """Extract content key (displayName, type, method) from request body."""
        if not isinstance(request_body, dict):
            return None
        display_name = request_body.get("displayName")
        item_type = request_body.get("type")
        if display_name and item_type:
            return (display_name, item_type, method)
        return None

    def add_trace(self, request: HTTPRequest, response: HTTPResponse):
        """Add a trace entry to all relevant indices."""
        parsed_url = urlparse(request.url)
        route = parsed_url.path
        if parsed_url.query:
            route += f"?{parsed_url.query}"

        normalized_key = f"{request.method} {self.normalize_route(route)}"

        # Index by normalized route
        self.by_route[normalized_key].append((request, response))

        # Index by content for POST/PATCH requests to /items
        if request.method in ("POST", "PATCH") and request.body:
            content_key = self.extract_content_key(request.body, request.method)
            if content_key:
                self.by_content[content_key].append((request, response))

        # Index by operation ID
        op_id = response.headers.get("x-ms-operation-id")
        if op_id and response.status_code == 202:
            self.by_operation[op_id]["post"] = (request, response)

        # Index operation poll/result responses
        op_match = OPERATION_PATTERN.search(route)
        if op_match:
            op_id = op_match.group(1)
            is_result = op_match.group(2) == "/result"
            if is_result:
                self.by_operation[op_id]["result"] = (request, response)
            else:
                self.by_operation[op_id]["poll"].append((request, response))

        # Track items from responses
        if response.body and isinstance(response.body, dict):
            item_id = response.body.get("id")
            if item_id and response.body.get("type"):
                self.items[item_id] = response.body


class MockFabricAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler that serves mocked Fabric API responses with state tracking."""

    trace_index: ClassVar[TraceIndex] = TraceIndex()
    route_lock: ClassVar[threading.Lock] = threading.Lock()

    # Runtime state for the mock server
    # Maps operation_id -> poll_count (to transition Running -> Succeeded)
    operation_poll_counts: ClassVar[dict[str, int]] = {}
    # Maps (displayName, type) -> assigned operation_id for this test run
    content_to_operation: ClassVar[dict[tuple[str, str], str]] = {}

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

    def _read_request_body(self) -> Optional[dict]:
        """Read and parse JSON request body."""
        content_length = self.headers.get("Content-Length")
        if content_length:
            try:
                body_bytes = self.rfile.read(int(content_length))
                return json.loads(body_bytes.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        return None

    def _handle_request(self, method: str):
        """
        Handle HTTP request with stateful matching.

        Matching strategy:
        1. For POST /items: Match by (displayName, type) in request body
        2. For operation polls: Track poll count and transition state
        3. For other routes: Match by normalized route pattern
        """
        route = self.path
        route_key = f"{method} {route}"

        logger.info(f"Mock server received: {route_key}")

        # Read request body for POST/PATCH
        request_body = None
        if method in ("POST", "PATCH"):
            request_body = self._read_request_body()

        response = self._find_matching_response(method, route, request_body)

        if response is None:
            # For unknown routes, return a generic success response for mutation operations
            if method in ("PATCH", "DELETE"):
                logger.info(f"No trace found for {route_key}, returning generic 200 OK")
                response = HTTPResponse(
                    status_code=200,
                    headers={"Content-Type": "application/json"},
                    body={},
                    timestamp=None,
                )
            elif method == "POST":
                logger.info(f"No trace found for {route_key}, returning generic 202 Accepted")
                response = HTTPResponse(
                    status_code=202,
                    headers={"Content-Type": "application/json"},
                    body={},
                    timestamp=None,
                )
            else:
                logger.warning(f"No trace data found for {route_key}")
                self.send_error(404, f"No trace data found for {route_key}")
                return

        self._send_response(response, route_key)

    def _find_matching_response(self, method: str, route: str, request_body: Optional[dict]) -> Optional[HTTPResponse]:
        """Find the best matching response for the request."""
        normalized_key = f"{method} {self.trace_index.normalize_route(route)}"

        # Strategy 1: Handle operation polling with state machine
        op_match = OPERATION_PATTERN.search(route)
        if op_match:
            return self._handle_operation_request(op_match.group(1), op_match.group(2) == "/result")

        # Strategy 2: Content-based matching for POST /items
        if method == "POST" and route.endswith("/items") and request_body:
            content_key = self.trace_index.extract_content_key(request_body, method)
            if content_key:
                return self._handle_item_creation(content_key)

        # Strategy 3: Content-based matching for PATCH (updateDefinition, etc.)
        if method == "POST" and "updateDefinition" in route:
            # For updateDefinition, find any matching trace
            traces = self.trace_index.by_route.get(normalized_key, [])
            if traces:
                _, response = traces[-1]
                return response

        # Strategy 4: Route-based matching for other requests
        traces = self.trace_index.by_route.get(normalized_key, [])
        if traces:
            _, response = traces[-1]
            return response

        return None

    def _handle_item_creation(self, content_key: tuple[str, str, str]) -> Optional[HTTPResponse]:
        """
        Handle POST /items with content-based matching.

        Returns the appropriate response and sets up operation tracking if async.
        """
        display_name, item_type, method = content_key
        traces = self.trace_index.by_content.get(content_key, [])

        if not traces:
            logger.warning(f"No trace found for item creation: {display_name} ({item_type})")
            return None

        # Find a trace with a valid response
        for _request, response in traces:
            if response.status_code in (200, 201, 202):
                # For 202 responses, track the operation for subsequent polls
                if response.status_code == 202:
                    op_id = response.headers.get("x-ms-operation-id")
                    if op_id:
                        with self.route_lock:
                            self.content_to_operation[(display_name, item_type)] = op_id
                            self.operation_poll_counts[op_id] = 0
                        logger.info(f"Tracking async operation {op_id} for {display_name} ({item_type})")

                logger.info(f"Matched item creation: {display_name} ({item_type}) -> status {response.status_code}")
                return response

        # Return last response even if not successful (for error testing)
        _, response = traces[-1]
        return response

    def _handle_operation_request(self, operation_id: str, is_result: bool) -> Optional[HTTPResponse]:
        """
        Handle operation status polling with state machine.

        - First poll: Return "Running" status
        - Subsequent polls: Return "Succeeded" status with Location header for result
        - Result endpoint: Return the created item details
        """
        op_data = self.trace_index.by_operation.get(operation_id)

        if not op_data:
            # Try to find by normalized pattern - any operation ID
            for _known_op_id, known_data in self.trace_index.by_operation.items():
                if known_data["post"] or known_data["result"]:
                    op_data = known_data
                    break

        if not op_data:
            logger.warning(f"No operation data found for {operation_id}")
            return None

        if is_result:
            # Return the result (created item)
            if op_data["result"]:
                _, response = op_data["result"]
                logger.info(f"Returning operation result for {operation_id}")
                return response
            return None

        # Handle status poll
        with self.route_lock:
            poll_count = self.operation_poll_counts.get(operation_id, 0)
            self.operation_poll_counts[operation_id] = poll_count + 1

        # Find poll traces
        poll_traces = op_data.get("poll", [])
        if not poll_traces:
            logger.warning(f"No poll traces for operation {operation_id}")
            return None

        # State machine: first poll returns Running, subsequent returns Succeeded
        if poll_count == 0:
            # Find a "Running" response
            for _, response in poll_traces:
                if isinstance(response.body, dict) and response.body.get("status") == "Running":
                    logger.info(f"Operation {operation_id} poll #{poll_count}: Running")
                    return response
        else:
            # Find a "Succeeded" response
            for _, response in poll_traces:
                if isinstance(response.body, dict) and response.body.get("status") == "Succeeded":
                    logger.info(f"Operation {operation_id} poll #{poll_count}: Succeeded")
                    return response

        # Fallback to last poll response
        if poll_traces:
            _, response = poll_traces[-1]
            return response

        return None

    def _send_response(self, response: HTTPResponse, route_key: str):
        """Send the HTTP response."""
        self.send_response(response.status_code)

        if isinstance(response.body, dict):
            body_bytes = json.dumps(response.body).encode()
        elif isinstance(response.body, str) and response.body:
            body_bytes = response.body.encode()
        else:
            body_bytes = json.dumps({}).encode()

        # Forward relevant headers
        for header_name, header_value in response.headers.items():
            lower_name = header_name.lower()
            # Include operation headers for async operations
            if lower_name in ("x-ms-operation-id", "retry-after"):
                self.send_header(header_name, header_value)
            elif lower_name == "location":
                # Rewrite location to point to mock server
                if "operations" in header_value:
                    # Extract operation ID and rewrite URL
                    op_match = OPERATION_PATTERN.search(header_value)
                    if op_match:
                        op_id = op_match.group(1)
                        is_result = op_match.group(2) == "/result"
                        suffix = "/result" if is_result else ""
                        new_location = f"http://127.0.0.1:{MOCK_SERVER_PORT}/v1/operations/{op_id}{suffix}"
                        self.send_header("Location", new_location)
                else:
                    self.send_header(header_name, header_value)
            elif lower_name not in (
                "content-length",
                "content-encoding",
                "transfer-encoding",
                "server",
                "date",
                "home-cluster-uri",
                "request-redirected",
            ):
                self.send_header(header_name, header_value)

        self.send_header("Content-Length", len(body_bytes))
        self.end_headers()
        self.wfile.write(body_bytes)

        logger.debug(f"Mock server responded to {route_key}: status={response.status_code}")

    @classmethod
    def load_trace_data(cls, trace_file: Path):
        """
        Load trace data from JSON file and build indices.

        Args:
            trace_file: Path to the http_trace.json file
        """
        cls.trace_index = TraceIndex()
        cls.operation_poll_counts.clear()
        cls.content_to_operation.clear()

        with trace_file.open("r") as f:
            data = json.load(f)

        traces = data.get("traces", [])
        loaded_count = 0

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

                cls.trace_index.add_trace(request, response)
                loaded_count += 1

            except Exception as e:
                logger.warning(f"Failed to parse trace entry: {e}")
                continue

        logger.info(f"Loaded {loaded_count} traces into index")
        logger.info(f"  - Routes: {len(cls.trace_index.by_route)}")
        logger.info(f"  - Content keys: {len(cls.trace_index.by_content)}")
        logger.info(f"  - Operations: {len(cls.trace_index.by_operation)}")


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
