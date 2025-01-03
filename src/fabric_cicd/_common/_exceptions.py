import json
import logging


class ParsingError(Exception):
    def __init__(self, message):
        super().__init__(message)


class InputError(Exception):
    def __init__(self, message):
        super().__init__(message)


class TokenError(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvokeError(Exception):
    def __init__(self, message, response, method, url, body, logger):
        super().__init__(message)
        log_invoke_payload(logger, response, method, url, body, error=True)


def log_invoke_payload(logger, response, method, url, body, error=False):
    debug_message = [
        f"\nURL: {url}",
        f"Method: {method}",
        (
            f"Request Body:\n{json.dumps(body, indent=4)}"
            if body
            else "Request Body: None"
        ),
    ]
    if response is not None:
        debug_message.extend(
            [
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
            ]
        )

    # Join all parts into a single message
    full_debug_message = "\n".join(debug_message)
    if error:
        logger.error(full_debug_message)
    elif logger.isEnabledFor(logging.DEBUG):
        logger.debug(full_debug_message)
    else:
        logger.info(full_debug_message)
