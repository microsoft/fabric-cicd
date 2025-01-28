import json

import requests


class FabricRequest:
    def __init__(self, method, url, bearer_token, body=None, files=None):
        self.request_method = method
        self.request_url = url
        self._request_body = body
        self.request_files = files
        self.request_headers = {"Authorization": f"Bearer {bearer_token}"}
        if files is None:
            self.request_headers["Content-Type"] = "application/json; charset=utf-8"

        self._response = None

    def _unpack(self):
        return {
            "method": self.request_method,
            "url": self.request_url,
            "headers": self.request_headers,
            "json": self.request_body,
            "files": self.request_files,
        }

    @property
    def request_body(self):
        return self._request_body if self._request_body else {}

    @property
    def log_message(self):
        message = [
            f"\nURL: {self.request_url}",
            f"Method: {self.request_method}",
            f"Request Body:\n{self.request_body}",
        ]
        if self.response is not None:
            message.extend([
                f"Response Status: {self.response.status_code}",
                "Response Headers:",
                json.dumps(dict(self.response.headers), indent=4),
                "Response Body:",
                (
                    json.dumps(self.response.json(), indent=4)
                    if self.response.headers.get("Content-Type") == "application/json"
                    else self.response.text
                ),
                "",
            ])

        return "\n".join(message)

    @property
    def retry_after(self):
        if self.response and self.response.headers.get("Retry-After", None):
            return self.response.headers.get("Retry-After")
        return 60

    @property
    def response_location(self):
        return self.response.headers.get("Location", None)

    def submit(self):
        self._response = requests.request(**self._unpack())
