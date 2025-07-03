# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Handles interactions with the Fabric API, including authentication and request management."""

import base64
import datetime
import json
import logging
import time
from typing import Optional

import requests
from azure.core.credentials import TokenCredential
from azure.core.exceptions import (
    ClientAuthenticationError,
)

import fabric_cicd.constants as constants
from fabric_cicd._common._exceptions import InvokeError, TokenError

logger = logging.getLogger(__name__)


class FabricEndpoint:
    """Handles interactions with the Fabric API, including authentication and request management."""

    def __init__(self, token_credential: TokenCredential, requests_module: requests = requests) -> None:
        """
        Initializes the FabricEndpoint instance, sets up the authentication token.

        Args:
            token_credential: The token credential.
            requests_module: The requests module.
        """
        self.aad_token = None
        self.aad_token_expiration = None
        self.token_credential = token_credential
        self.requests = requests_module
        self._refresh_token()

    def invoke(self, method: str, url: str, body: str = "{}", files: Optional[dict] = None, **kwargs) -> dict:
        """
        Sends an HTTP request to the specified URL with the given method and body.

        Args:
            method: HTTP method to use for the request (e.g., 'GET', 'POST', 'PATCH', 'DELETE').
            url: URL to send the request to.
            body: The JSON body to include in the request. Defaults to an empty JSON object.
            files: The file path to be included in the request. Defaults to None.
            **kwargs: Additional keyword arguments to pass to the method.
        """
        exit_loop = False
        iteration_count = 0
        long_running = False
        start_time = time.time()
        invoke_log_message = ""

        while not exit_loop:
            try:
                headers = {
                    "Authorization": f"Bearer {self.aad_token}",
                    "User-Agent": f"{constants.USER_AGENT}",
                }
                if files is None:
                    headers["Content-Type"] = "application/json; charset=utf-8"
                response = self.requests.request(method=method, url=url, headers=headers, json=body, files=files)

                iteration_count += 1

                invoke_log_message = _format_invoke_log(response, method, url, body)

                # Handle expired authentication token
                if response.status_code == 401 and response.headers.get("x-ms-public-api-error-code") == "TokenExpired":
                    logger.info(f"{constants.INDENT}AAD token expired. Refreshing token.")
                    self._refresh_token()
                else:
                    exit_loop, method, url, body, long_running = _handle_response(
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

        end_time = time.time()
        logger.debug(f"Request completed in {end_time - start_time} seconds")

        return {
            "header": dict(response.headers),
            "body": (response.json() if "application/json" in response.headers.get("Content-Type") else {}),
            "status_code": response.status_code,
        }

    def _refresh_token(self) -> None:
        """Refreshes the AAD token if empty or expiration has passed."""
        if (
            self.aad_token is None
            or self.aad_token_expiration is None
            or self.aad_token_expiration < datetime.datetime.utcnow()
        ):
            resource_url = "https://api.fabric.microsoft.com/.default"

            try:
                self.aad_token = self.token_credential.get_token(resource_url).token
            except ClientAuthenticationError as e:
                msg = f"Failed to acquire AAD token. {e}"
                raise TokenError(msg, logger) from e
            except Exception as e:
                msg = f"An unexpected error occurred when generating the AAD token. {e}"
                raise TokenError(msg, logger) from e

            try:
                decoded_token = _decode_jwt(self.aad_token)
                expiration = decoded_token.get("exp")
                upn = decoded_token.get("upn")
                appid = decoded_token.get("appid")
                oid = decoded_token.get("oid")

                if expiration:
                    self.aad_token_expiration = datetime.datetime.fromtimestamp(expiration)
                else:
                    msg = "Token does not contain expiration claim."
                    raise TokenError(msg, logger)

                if upn:
                    _log_executing_identity(f"Executing as User '{upn}'")
                    self.upn_auth = True
                else:
                    self.upn_auth = False
                    if appid:
                        _log_executing_identity(f"Executing as Application Id '{appid}'")
                    elif oid:
                        _log_executing_identity(f"Executing as Object Id '{oid}'")

            except Exception as e:
                msg = f"An unexpected error occurred while decoding the credential token. {e}"
                raise TokenError(msg, logger) from e


def _log_executing_identity(msg: str) -> None:
    if "disable_print_identity" not in constants.FEATURE_FLAG:
        logger.info(msg)


def _handle_response(
    response: requests.Response,
    method: str,
    url: str,
    body: str,
    long_running: bool,
    iteration_count: int,
) -> tuple:
    """
    Handles the response from an HTTP request, including retries, throttling, and token expiration.
    Technical debt: this method needs to be refactored to be more testable and requires less parameters.
    Initial approach is only temporary to support testing, but only temporary.

    Args:
        response: The response object from the HTTP request.
        method: The HTTP method used in the request.
        url: The URL used in the request.
        body: The JSON body used in the request.
        long_running: A boolean indicating if the operation is long-running.
        iteration_count: The current iteration count of the loop.
    """
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
                    prepend_message=f"{constants.INDENT}Operation in progress.",
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
        and response.headers.get("x-ms-public-api-error-code") == "ItemDisplayNameNotAvailableYet"
    ):
        handle_retry(
            attempt=iteration_count,
            base_delay=30,
            max_retries=5,
            response_retry_after=300,
            prepend_message="Item name is reserved.",
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
            f" Message: {response.json()['message']}.  {response.json().get('moreDetails', '')}"
            if "application/json" in (response.headers.get("Content-Type") or "")
            else ""
        )
        msg = f"Unhandled error occurred calling {method} on '{url}'.{err_msg}"
        raise Exception(msg)

    return exit_loop, method, url, body, long_running


def handle_retry(
    attempt: int,
    base_delay: float,
    response_retry_after: float = 60,
    prepend_message: str = "",
    max_retries: int | None = None,
) -> None:
    """
    Handles retry logic with exponential backoff based on the response.

    Args:
        attempt: The current attempt number.
        base_delay: Base delay in seconds for backoff.
        response_retry_after: The value of the Retry-After header from the response.
        prepend_message: Message to prepend to the retry log.
        max_retries: Maximum number of retry attempts. If None, retries indefinitely.
    """
    if max_retries is None or attempt < max_retries:
        retry_after = float(response_retry_after)
        base_delay = float(base_delay)
        delay = min(retry_after, base_delay * (2**attempt))

        # modify output for proper plurality and formatting
        delay_str = f"{delay:.0f}" if delay.is_integer() else f"{delay:.2f}"
        second_str = "second" if delay == 1 else "seconds"
        prepend_message += " " if prepend_message else ""

        logger.info(
            f"{constants.INDENT}{prepend_message}Checking again in {delay_str} {second_str} (Attempt {attempt})..."
        )
        time.sleep(delay)
    else:
        msg = f"Maximum retry attempts ({max_retries}) exceeded."
        raise Exception(msg)


def _decode_jwt(token: str) -> dict:
    """
    Decodes a JWT token and returns the payload as a dictionary.

    Args:
        token: The JWT token to decode.
    """
    try:
        # Split the token into its parts
        parts = token.split(".")
        if len(parts) != 3:
            msg = "The token has an invalid JWT format"
            raise TokenError(msg, logger)

        # Decode the payload (second part of the token)
        payload = parts[1]
        padding = "=" * (4 - len(payload) % 4)
        payload += padding
        decoded_bytes = base64.urlsafe_b64decode(payload.encode("utf-8"))
        decoded_str = decoded_bytes.decode("utf-8")
        return json.loads(decoded_str)
    except Exception as e:
        msg = f"An unexpected error occurred while decoding the credential token. {e}"
        raise TokenError(msg, logger) from e


def _format_invoke_log(response: requests.Response, method: str, url: str, body: str) -> str:
    """
    Format the log message for the invoke method.

    Args:
        response: The response object from the HTTP request.
        method: The HTTP method used in the request.
        url: The URL used in the request.
        body: The JSON body used in the request.
    """
    message = [
        f"\nURL: {url}",
        f"Method: {method}",
        (f"Request Body:\n{json.dumps(body, indent=4)}" if body else "Request Body: None"),
    ]
    if response is not None:
        message.extend([
            f"Response Status: {response.status_code}",
            "Response Headers:",
            json.dumps(dict(response.headers), indent=4),
            "Response Body:",
            (
                json.dumps(response.json(), indent=4)
                if response.headers.get("Content-Type") == "application/json"
                else response.text
            ),
            "",
        ])

    return "\n".join(message)
