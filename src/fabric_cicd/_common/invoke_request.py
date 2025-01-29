# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging
import time

import requests

from fabric_cicd._common._exceptions import InvokeError

logger = logging.getLogger(__name__)


class InvokeRequest:
    """Class to wrap the request and response of an Invoke call."""

    def __init__(self, fabric_endpoint_obj, method, url, body="{}", files=None):
        self.fabric_endpoint_obj = fabric_endpoint_obj
        self.method = method
        self.url = url
        self.body = body
        self.files = files
        self.headers = {"Authorization": f"Bearer {fabric_endpoint_obj.aad_token}"}
        if files is None:
            self.headers["Content-Type"] = "application/json; charset=utf-8"
        self.response = None
        self.log_message = None

    def submit(self):
        try:
            self.response = requests.request(
                method=self.method, url=self.url, headers=self.headers, json=self.body, files=self.files
            )
            self._create_log_message()

        except Exception as e:
            if self.log_message is None:
                self._create_log_message()
            logger.debug(self.log_message)
            raise InvokeError(e, logger, self.log_message) from e

        self.process_response()

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(self.log_message)

    def retry_submit(self, wait=False):
        if wait:
            time.sleep(1)
        else:
            self.submit()

    def process(self):
        # Recursive calls
        self.process_sucess()
        self.process_token_expiration()
        self.process_exception()

    def process_token_expiration(self):
        # Recursive Call
        if (
            self.response.status_code == 401
            and self.response.headers.get("x-ms-public-api-error-code") == "TokenExpired"
        ):
            logger.info("AAD token expired. Refreshing token.")
            self.fabric_endpoint_obj._refresh_token()
            self.retry_submit()

    def process_sucess(self):
        if self.response.status_code in {200, 201} or (
            # Valid response for environmentlibrariesnotfound
            self.response.status_code == 404
            and self.response.headers.get("x-ms-public-api-error-code") == "EnvironmentLibrariesNotFound"
        ):
            logger.debug("Successful call.")

    def process_exception(self):
        if self.response.status_code == 400 and "is not present in the environment." in response.json().get(
            "message", "No message provided"
        ):
            msg = (
                f"Deployment attempted to remove a library that is not present in the environment. "
                f"Description: {self.response.json().get('message')}"
            )
            raise Exception(msg)

        # Handle unsupported principal type
        if (
            self.response.status_code == 400
            and self.response.headers.get("x-ms-public-api-error-code") == "PrincipalTypeNotSupported"
        ):
            msg = f"The executing principal type is not supported to call {self.method} on '{self.url}'."
            raise Exception(msg)

        # Handle unsupported item types
        if self.response.status_code == 403 and self.response.reason == "FeatureNotAvailable":
            msg = f"Item type not supported. Description: {self.response.reason}"
            raise Exception(msg)

        # Handle unexpected errors
        err_msg = (
            f" Message: {self.response.json()['message']}"
            if "application/json" in (self.response.headers.get("Content-Type") or "")
            else ""
        )
        msg = f"Unhandled error occurred calling {self.method} on '{self.url}'.{err_msg}"
        raise Exception(msg)

    def invoke(self, invoke_obj: InvokeRequest):
        try:
            # Handle expired authentication token
            if response.status_code == 401 and response.headers.get("x-ms-public-api-error-code") == "TokenExpired":
                logger.info("AAD token expired. Refreshing token.")
                self._refresh_token()
                self.invoke(method, url, body, files, iteration_count=iteration_count + 1, **kwargs)
            else:
                _handle_response(
                    self,
                    response,
                    method,
                    url,
                    body,
                    long_running,
                    iteration_count,
                    **kwargs,
                )

            # Log if reached to end of loop iteration
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(invoke_log_message)

        except Exception as e:
            logger.debug(invoke_log_message)
            raise InvokeError(e, logger, invoke_log_message) from e

        return {
            "header": dict(response.headers),
            "body": (response.json() if "application/json" in response.headers.get("Content-Type") else {}),
            "status_code": response.status_code,
        }

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


def _handle_response(response, method, url, body, long_running, iteration_count, **kwargs):
    exit_loop = False
    retry_after = response.headers.get("Retry-After", 60)

    # Handle long-running operations
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-result
    if (response.status_code == 200 and long_running) or response.status_code == 202:
        url = response.headers.get("Location")
        method = "GET"
        body = "{}"
        response_json = response.json()

        if long_running:
            status = response_json.get("status")
            if status == "Succeeded":
                long_running = False
                # If location not included in operation success call, no body is expected to be returned
                exit_loop = url is None

            elif status == "Failed":
                response_error = response_json["error"]
                msg = (
                    f"Operation failed. Error Code: {response_error['errorCode']}. "
                    f"Error Message: {response_error['message']}"
                )
                raise Exception(msg)
            elif status == "Undefined":
                msg = f"Operation is in an undefined state. Full Body: {response_json}"
                raise Exception(msg)
            else:
                handle_retry(
                    attempt=iteration_count - 1,
                    base_delay=0.5,
                    response_retry_after=retry_after,
                    max_retries=kwargs.get("max_retries", 5),
                    prepend_message="Operation in progress.",
                )
        else:
            time.sleep(1)
            long_running = True

    # Handle successful responses
    elif response.status_code in {200, 201} or (
        # Valid response for environmentlibrariesnotfound
        response.status_code == 404
        and response.headers.get("x-ms-public-api-error-code") == "EnvironmentLibrariesNotFound"
    ):
        exit_loop = True

    # Handle API throttling
    elif response.status_code == 429:
        handle_retry(
            attempt=iteration_count,
            base_delay=10,
            max_retries=5,
            response_retry_after=retry_after,
            prepend_message="API is throttled.",
        )

    # Handle unauthorized access
    elif response.status_code == 401 and response.headers.get("x-ms-public-api-error-code") == "Unauthorized":
        msg = f"The executing identity is not authorized to call {method} on '{url}'."
        raise Exception(msg)

    # Handle item name conflicts
    elif (
        response.status_code == 400
        and response.headers.get("x-ms-public-api-error-code") == "ItemDisplayNameAlreadyInUse"
    ):
        handle_retry(
            attempt=iteration_count,
            base_delay=2.5,
            max_retries=5,
            prepend_message="Item name is reserved. ",
        )

    # Handle scenario where library removed from environment before being removed from repo
    elif response.status_code == 400 and "is not present in the environment." in response.json().get(
        "message", "No message provided"
    ):
        msg = (
            f"Deployment attempted to remove a library that is not present in the environment. "
            f"Description: {response.json().get('message')}"
        )
        raise Exception(msg)

    # Handle unsupported principal type
    elif (
        response.status_code == 400
        and response.headers.get("x-ms-public-api-error-code") == "PrincipalTypeNotSupported"
    ):
        msg = f"The executing principal type is not supported to call {method} on '{url}'."
        raise Exception(msg)

    # Handle unsupported item types
    elif response.status_code == 403 and response.reason == "FeatureNotAvailable":
        msg = f"Item type not supported. Description: {response.reason}"
        raise Exception(msg)

    # Handle unexpected errors
    else:
        err_msg = (
            f" Message: {response.json()['message']}"
            if "application/json" in (response.headers.get("Content-Type") or "")
            else ""
        )
        msg = f"Unhandled error occurred calling {method} on '{url}'.{err_msg}"
        raise Exception(msg)

    return exit_loop, method, url, body, long_running


def handle_retry(attempt, base_delay, max_retries, response_retry_after=60, prepend_message=""):
    """
    Handles retry logic with exponential backoff based on the response.

    :param attempt: The current attempt number.
    :param base_delay: Base delay in seconds for backoff.
    :param max_retries: Maximum number of retry attempts.
    :param response_retry_after: The value of the Retry-After header from the response.
    :param prepend_message: Message to prepend to the retry log.
    """
    if attempt < max_retries:
        retry_after = float(response_retry_after)
        base_delay = float(base_delay)
        delay = min(retry_after, base_delay * (2**attempt))

        # modify output for proper plurality and formatting
        delay_str = f"{delay:.0f}" if delay.is_integer() else f"{delay:.2f}"
        second_str = "second" if delay == 1 else "seconds"
        prepend_message += " " if prepend_message else ""

        logger.info(f"{prepend_message}Checking again in {delay_str} {second_str} (Attempt {attempt}/{max_retries})...")
        time.sleep(delay)
    else:
        msg = f"Maximum retry attempts ({max_retries}) exceeded."
        raise Exception(msg)
