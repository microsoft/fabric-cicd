# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import base64
import datetime
import json
import logging
import time

import requests
from azure.core.exceptions import (
    ClientAuthenticationError,
)

from fabric_cicd._common._exceptions import InvokeError, TokenError

logger = logging.getLogger(__name__)


class FabricEndpoint:
    """Handles interactions with the Fabric API, including authentication and request management."""

    def __init__(self, token_credential):
        """Initializes the FabricEndpoint instance, sets up the authentication token."""
        self.aad_token = None
        self.aad_token_expiration = None
        self.token_credential = token_credential
        self._refresh_token()

    def invoke(self, method, url, body="{}"):
        """
        Sends an HTTP request to the specified URL with the given method and body.

        :param method: HTTP method to use for the request (e.g., 'GET', 'POST', 'PATCH', 'DELETE').
        :param url: URL to send the request to.
        :param body: The JSON body to include in the request. Defaults to an empty JSON object.
        :return: A dictionary containing the response headers, body, and status code.
        """
        exit_loop = False
        iteration_count = 0
        long_running = False

        while not exit_loop:
            try:
                headers = {
                    "Authorization": f"Bearer {self.aad_token}",
                    "Content-Type": "application/json; charset=utf-8",
                }

                response = requests.request(method=method, url=url, headers=headers, json=body)
                iteration_count += 1

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
                            exit_loop = True
                        elif status == "Failed":
                            response_error = response_json["error"]
                            msg = f"Operation failed. Error Code: {response_error['errorCode']}. Error Message: {response_error['message']}"
                            raise Exception(msg)
                        elif status == "Undefined":
                            msg = f"Operation is in an undefined state. Full Body: {response_json}"
                            raise Exception(msg)
                        else:
                            retry_after = float(response.headers.get("Retry-After", 0.5))
                            logger.info(f"Operation in progress. Checking again in {retry_after} seconds.")
                            time.sleep(retry_after)
                    else:
                        time.sleep(1)
                        long_running = True

                # Handle successful responses
                elif response.status_code in {200, 201}:
                    exit_loop = True

                # Handle API throttling
                elif response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 5)) + 5
                    logger.info(f"API Overloaded: Retrying in {retry_after} seconds")
                    time.sleep(retry_after)

                # Handle expired authentication token
                elif (
                    response.status_code == 401 and response.headers.get("x-ms-public-api-error-code") == "TokenExpired"
                ):
                    logger.info("AAD token expired. Refreshing token.")
                    self._refresh_token()

                # Handle unauthorized access
                elif (
                    response.status_code == 401 and response.headers.get("x-ms-public-api-error-code") == "Unauthorized"
                ):
                    msg = f"The executing identity is not authorized to call {method} on '{url}'."
                    raise Exception(msg)

                # Handle item name conflicts
                elif (
                    response.status_code == 400
                    and response.headers.get("x-ms-public-api-error-code") == "ItemDisplayNameAlreadyInUse"
                ):
                    if iteration_count <= 6:
                        logger.info("Item name is reserved. Retrying in 60 seconds.")
                        time.sleep(60)
                    else:
                        msg = f"Item name still in use after 6 attempts. Description: {response.reason}"
                        raise Exception(msg)

                # Handle unsupported principal type
                elif (
                    response.status_code == 400
                    and response.headers.get("x-ms-public-api-error-code") == "PrincipalTypeNotSupported"
                ):
                    msg = f"The executing principal type is not supported to call {method} on '{url}'"
                    raise Exception(msg)

                # Handle unsupported item types
                elif response.status_code == 403 and response.reason == "FeatureNotAvailable":
                    msg = f"Item type not supported. Description: {response.reason}"
                    raise Exception(msg)

                # Handle unexpected errors
                else:
                    err_msg = _parse_json_body(response, f" Message: {response.json()['message']}", "")
                    msg = f"Unhandled error occurred calling {method} on '{url}'.{err_msg}"
                    raise Exception(msg)

                # Log if reached to end of loop iteration
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(invoke_log_message)

            except Exception as e:
                logger.debug(invoke_log_message)
                raise InvokeError(e, logger, invoke_log_message) from e

        return {
            "header": dict(response.headers),
            "body": (_parse_json_body(response, response.json(), {})),
            "status_code": response.status_code,
        }

    def _refresh_token(self):
        """Refreshes the AAD token if empty or expiration has passed"""
        if (
            self.aad_token is None
            or self.aad_token_expiration is None
            or self.aad_token_expiration < datetime.datetime.utcnow()
        ):
            resource_url = "https://api.fabric.microsoft.com/.default"

            try:
                self.aad_token = self.token_credential.get_token(resource_url).token
            except ClientAuthenticationError as e:
                msg = f"Failed to aquire AAD token. {e}"
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
                    logger.info(f"Executing as User '{upn}'")
                    self.upn_auth = True
                else:
                    self.upn_auth = False
                    if appid:
                        logger.info(f"Executing as Application Id '{appid}'")
                    elif oid:
                        logger.info(f"Executing as Object Id '{oid}'")

            except Exception as e:
                msg = f"An unexpected error occurred while decoding the credential token. {e}"
                raise TokenError(msg, logger) from e


def _decode_jwt(token):
    """Decodes a JWT token and returns the payload as a dictionary."""
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
            _parse_json_body(response, json.dumps(response.json(), indent=4), response.text),
            "",
        ])

    return "\n".join(message)


def _parse_json_body(response, default_return, alt_return):
    return default_return if response.headers.get("Content-Type") == "application/json" else alt_return
