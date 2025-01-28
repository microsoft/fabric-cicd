# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import logging
import time

from fabric_cicd._common._exceptions import InvokeError
from fabric_cicd._common._fabric_request import FabricRequest

logger = logging.getLogger(__name__)


class FabricEndpoint:
    """Handles interactions with the Fabric API, including authentication and request management."""

    def invoke(self, method, url, body=None, files=None, **kwargs):
        exit_loop = False
        iteration_count = 0
        long_running = False

        while not exit_loop:
            try:
                iteration_count += 1
                request_obj = FabricRequest(
                    method=method,
                    url=url,
                    bearer_token=self.aad_token,
                    body=body,
                    files=files,
                )
                request_obj.submit()

                if not long_running and request_obj.response.status_code in {202}:
                    time.sleep(1)
                elif (
                    request_obj.response.status_code == 200 and long_running
                ) or request_obj.response.status_code == 202:
                    max_retries = kwargs.get("max_longrunning_retries", 5)
                    exit_loop, new_url = handle_longrunning(request_obj, iteration_count, max_retries)

                # handle response

                if long_running:
                    request_obj = FabricRequest(
                        method="GET",
                        url=new_url,
                        bearer_token=None,
                        body="{}",
                    )

            except Exception as e:
                logger.debug(request_obj.log_message)
                raise InvokeError(e, logger, request_obj.log_message) from e


def handle_longrunning(request_obj, iteration_count, max_retries):
    # Handle long-running operations
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-result

    response_json = request_obj.response.json()
    status = response_json.get("status")
    location_url = request_obj.response_location

    if status == "Succeeded":
        return location_url is None, location_url

    if status == "Failed":
        response_error = response_json.get("error", {})
        msg = (
            f"Operation failed. Error Code: {response_error.get('errorCode')}. "
            f"Error Message: {response_error.get('message')}"
        )
        raise Exception(msg)

    if status == "Undefined":
        msg = f"Operation is in an undefined state. Full Body: {response_json}"
        raise Exception(msg)

    handle_retry(
        attempt=iteration_count - 1,
        base_delay=0.5,
        response_retry_after=request_obj.retry_after,
        max_retries=max_retries,
        prepend_message="Operation in progress.",
    )
    return False, location_url

    def handle_response(self, response, method, url, body, iteration_count, long_running):
        exit_loop = False
        retry_after = response.headers.get("Retry-After", 60)
        invoke_log_message = _format_invoke_log(response, method, url, body)

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

        # Handle expired authentication token
        elif response.status_code == 401 and response.headers.get("x-ms-public-api-error-code") == "TokenExpired":
            logger.info("AAD token expired. Refreshing token.")
            self._refresh_token()

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

        # Log if reached to end of loop iteration
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(invoke_log_message)


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


def _format_invoke_log(response, method, url, body):
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
