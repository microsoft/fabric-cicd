# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging
import time

import requests

logger = logging.getLogger(__name__)

from fabric_cicd._common._exceptions import (
    EnvironmentLibrariesNotFoundError,
    EnvironmentValidationError,
    ExceededMaxRetryError,
    LongRunningOperationError,
    UnhandledInvokeError,
    UnsupportedItemTypeError,
    UnsupportedPrincipalTypeError,
)


class FabricRequest:
    """Class to wrap the request and response of an Invoke call."""

    def __init__(self, endpoint, method, url, body="{}", files=None, max_retries=5):
        self.endpoint = endpoint
        self.method = method
        self.url = url
        self.body = body
        self.files = files
        self.headers = {"Authorization": f"Bearer {endpoint.aad_token}"}
        if files is None:
            self.headers["Content-Type"] = "application/json; charset=utf-8"
        self.response = None
        self.log_message = None
        self.iteration_count = 1
        self.max_retries = max_retries
        self.retry_after = 60
        self.status_code = None
        self.api_error_code = None

    def submit(self):
        exception_occurred = False
        try:
            self.response = requests.request(
                method=self.method, url=self.url, headers=self.headers, json=self.body, files=self.files
            )
            self.retry_after = self.response.headers.get("Retry-After", 60)
            self.status_code = self.response.status_code
            self.api_error_code = self.response.headers.get("x-ms-public-api-error-code")

            self._create_log_message()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(self.log_message)

            self.process_response()

        except Exception:
            logger.debug(self.log_message)
            exception_occurred = True
            raise
        finally:
            if logger.isEnabledFor(logging.DEBUG) and not exception_occurred:
                logger.debug(self.log_message)

        return {
            "header": dict(self.response.headers),
            "body": (self.response.json() if "application/json" in self.response.headers.get("Content-Type") else {}),
            "status_code": self.response.status_code,
        }

    def retry_submit(self):
        self.iteration_count += 1
        self.submit()

    def process(self):
        self.process_success()
        self.process_token_expiration()  # recursive call
        self.process_exception()
        self.process_long_running()

    def process_success(self):
        if (
            # Success
            self.status_code == 201
            # OK Success without redirect location
            or (
                self.status_code == 200
                and self.response.headers.get("Location") is None
                and self.response.json.get("status") == "Succeeded"
            )
            # Valid response for environmentlibrariesnotfound
            or (
                # Valid response for environmentlibrariesnotfound
                self.status_code == 404 and self.api_error_code == "EnvironmentLibrariesNotFound"
            )
        ):
            logger.debug("Successful call.")

    def process_expired_token(self):
        if self.status_code == 401 and self.api_error_code == "TokenExpired":
            logger.info("AAD token expired. Refreshing token.")
            self.fabric_endpoint_obj._refresh_token()
            self.retry_submit()

    def process_throttle(self):
        if self.status_code == 429:
            self._retry_wait(base_delay=10, prepend_message="API is throttled.")
            self.retry_submit()

    def process_exception(self):
        if self.status_code == 400 and self.api_error_code == "EnvironmentValidationFailed":
            msg = f"{self.response.json().get('message')}"
            raise EnvironmentValidationError(msg, logger)

        # Handle unsupported principal type
        if self.response.status_code == 400 and self.api_error_code == "PrincipalTypeNotSupported":
            msg = f"The executing principal type is not supported to call {self.method} on '{self.url}'."
            raise UnsupportedPrincipalTypeError(msg, logger)

        if self.response.status_code == 404 and self.api_error_code == "EnvironmentLibrariesNotFound":
            msg = "No libraries were found in Environment."
            raise EnvironmentLibrariesNotFoundError(msg, logger)

        # Handle unsupported item types
        if self.response.status_code == 403 and self.response.reason == "FeatureNotAvailable":
            msg = f"Item type not supported. Description: {self.response.reason}"
            raise UnsupportedItemTypeError(msg, logger)

        # Handle unexpected errors
        err_msg = (
            f" Message: {self.response.json()['message']}"
            if "application/json" in (self.response.headers.get("Content-Type") or "")
            else ""
        )
        msg = f"Unhandled error occurred calling {self.method} on '{self.url}'.{err_msg}"
        raise UnhandledInvokeError(msg, logger)

    def process_long_running(self):
        location = self.response.headers.get("Location")
        status = self.response.json.get("status")

        if self.status_code == 200 and (location is not None or status != "Succeeded"):
            status = self.response.json.get("status")
            response_json = self.response.json()

            if status == "Failed":
                response_error = response_json["error"]
                msg = (
                    f"Operation failed. Error Code: {response_error['errorCode']}. "
                    f"Error Message: {response_error['message']}"
                )
                raise LongRunningOperationError(msg, logger)
            if status == "Undefined":
                msg = f"Operation is in an undefined state. Full Body: {response_json}"
                raise LongRunningOperationError(msg, logger)

            self.method = "GET"
            self.url = location
            self.body = "{}"

            self._retry_wait(base_delay=0.5, prepend_message="Operation in progress.")

            self.retry_submit()

    def _create_log_message(self):
        message = [
            f"\nURL: {self.url}",
            f"Method: {self.method}",
            (f"Request Body:\n{json.dumps(self.body, indent=4)}" if self.body else "Request Body: None"),
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

        self.log_message = "\n".join(message)

    def _retry_wait(self, base_delay, max_retries=None, prepend_message=""):
        """
        Handles retry logic with exponential backoff based on the response.

        :param base_delay: Base delay in seconds for backoff.
        :param max_retries: Maximum number of retry attempts.
        :param prepend_message: Message to prepend to the retry log.
        """
        max_retries = max_retries if max_retries is not None else self.max_retries

        if self.iteration_count < self.max_retries:
            retry_after = float(self.retry_after)
            base_delay = float(base_delay)
            delay = min(retry_after, base_delay * (2**self.iteration_count))

            # modify output for proper plurality and formatting
            delay_str = f"{delay:.0f}" if delay.is_integer() else f"{delay:.2f}"
            second_str = "second" if delay == 1 else "seconds"
            prepend_message += " " if prepend_message else ""

            logger.info(
                f"{prepend_message}Checking again in {delay_str} {second_str} (Attempt {self.iteration_count}/{max_retries})..."
            )
            time.sleep(delay)
        else:
            msg = f"Maximum retry attempts ({max_retries}) exceeded."
            raise ExceededMaxRetryError(msg, logger)
