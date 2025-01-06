import base64
import datetime
import json
import time

import requests
from azure.identity import DefaultAzureCredential

from fabric_cicd._common._custom_print import print_header, print_line, print_sub_line


class FabricEndpoint:
    """
    Handles interactions with the Fabric API, including authentication and request management.
    """

    def __init__(self, debug_output=False):
        """
        Initializes the FabricEndpoint instance, sets up the authentication token, and sets debug mode.

        :param debug_output: If True, enables debug output for API requests.
        """
        self.aad_token = None
        self.aad_token_expiration = None
        self.debug_output = debug_output
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
            headers = {
                "Authorization": f"Bearer {self.aad_token}",
                "Content-Type": "application/json; charset=utf-8",
            }

            response = requests.request(method=method, url=url, headers=headers, json=body)
            iteration_count += 1

            if self.debug_output:
                self._write_debug_output(response, method, url, body)

            # Handle long-running operations
            # https://learn.microsoft.com/en-us/rest/api/fabric/core/long-running-operations/get-operation-result
            if (response.status_code == 200 and long_running) or response.status_code == 202:
                url = response.headers.get("Location")
                method = "GET"
                body = "{}"
                if long_running and response.json().get("status") in [
                    "Succeeded",
                    "Failed",
                    "Undefined",
                ]:
                    long_running = False
                elif not long_running:
                    time.sleep(1)
                    long_running = True
                else:
                    retry_after = float(response.headers.get("Retry-After", 0.5))
                    print_sub_line(f"Operation in progress. Checking again in {retry_after} seconds.")
                    time.sleep(retry_after)

            # Handle successful responses
            elif response.status_code in {200, 201}:
                exit_loop = True

            # Handle API throttling
            elif response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", 5)) + 5
                print_sub_line(f"API Overloaded: Retrying in {retry_after} seconds")
                time.sleep(retry_after)

            # Handle expired authentication token
            elif response.status_code == 401 and response.headers.get("x-ms-public-api-error-code") == "TokenExpired":
                print_sub_line("AAD token expired. Refreshing token.")
                self._refresh_token()

            # Handle item name conflicts
            elif (
                response.status_code == 400
                and response.headers.get("x-ms-public-api-error-code") == "ItemDisplayNameAlreadyInUse"
            ):
                if iteration_count <= 6:
                    print_sub_line("Item name is reserved. Retrying in 60 seconds.")
                    time.sleep(60)
                else:
                    self._raise_invoke_exception(
                        f"Item name still in use after 6 attempts. Description: {response.reason}",
                        response,
                        method,
                        url,
                        body,
                    )

            # Handle unsupported item types
            elif response.status_code == 403 and response.reason == "FeatureNotAvailable":
                self._raise_invoke_exception(
                    f"Item type not supported. Description: {response.reason}",
                    response,
                    method,
                    url,
                    body,
                )

            # Handle unexpected errors
            else:
                self._raise_invoke_exception(
                    f"Unhandled error occurred. Description: {response.reason}",
                    response,
                    method,
                    url,
                    body,
                )

        return {
            "header": dict(response.headers),
            "body": (response.json() if "application/json" in response.headers.get("Content-Type") else {}),
            "status_code": response.status_code,
        }

    def _refresh_token(self):
        """
        Refreshes the AAD token if empty or expiration has passed
        """
        if (
            self.aad_token is None
            or self.aad_token_expiration is None
            or self.aad_token_expiration < datetime.datetime.utcnow()
        ):
            credential = DefaultAzureCredential()
            resource_url = "https://api.fabric.microsoft.com"

            self.aad_token = credential.get_token(resource_url).token

            try:
                parts = self.aad_token.split(".")
                payload = parts[1]
                padding = "=" * (4 - len(payload) % 4)
                payload += padding
                decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
                expiration = json.loads(decoded).get("exp")

                if expiration:
                    self.aad_token_expiration = datetime.datetime.fromtimestamp(expiration)
                else:
                    print("Token does not contain expiration claim.")

            except Exception as e:
                print(f"An error occurred: {e}")

    def _write_debug_output(self, response, method, url, body):
        """
        Outputs debug information if debug mode is enabled.

        :param response: The HTTP response object to log.
        :param method: HTTP method used for the request.
        :param url: URL used for the request.
        :param body: Body of the request.
        """
        debug_color = "gray"
        print_header("DEBUG OUTPUT", debug_color)
        print_line(f"URL: {url}", debug_color)
        print_line(f"Method: {method}", debug_color)
        print_line(f"Request Body: {body}", debug_color)
        if response is not None:
            print_line(f"Response Status: {response.status_code}", debug_color)
            print_line("Response Header:", debug_color)
            print_line(response.headers, debug_color)
            print_line("Response Body:", debug_color)
            print_line(response.text, debug_color)
            print_line("")

    def _raise_invoke_exception(self, message, response, method, url, body):
        """
        Raises an exception with a message and optionally outputs debug information.

        :param message: The error message to include in the exception.
        :param response: The HTTP response object to log.
        :param method: HTTP method used for the request.
        :param url: URL used for the request.
        :param body: Body of the request.
        """
        if self.debug_output:
            self._write_debug_output(response, method, url, body)
        raise Exception(message)
