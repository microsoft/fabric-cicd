# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions for validating environment variables used by fabric-cicd."""

import logging
import os
import re
from urllib.parse import urlparse

from fabric_cicd._common._exceptions import InputError

logger = logging.getLogger(__name__)

_VALID_API_HOSTNAME_REGEX = re.compile(
    r"^([\w-]+\.)*"
    r"(powerbi\.com"
    r"|fabric\.microsoft\.com"
    r")\Z"
)


def validate_api_url(env_var_name: str, default_value: str) -> str:
    """
    Validates and returns the API root URL from an environment variable.
    Ensures the URL uses HTTPS and targets a known Microsoft host.

    Args:
        env_var_name: The name of the environment variable.
        default_value: The default value if the environment variable is not set.

    Returns:
        The validated API root URL.
    """
    value = os.environ.get(env_var_name, default_value)

    if not isinstance(value, str) or not value.strip():
        msg = f"Environment variable '{env_var_name}' must resolve to a non-empty string."
        raise InputError(msg, logger)

    parsed = urlparse(value)

    if parsed.scheme != "https":
        msg = f"Environment variable '{env_var_name}' must use HTTPS scheme. Got: '{parsed.scheme}'"
        raise InputError(msg, logger)

    hostname = parsed.hostname or ""
    if not _VALID_API_HOSTNAME_REGEX.match(hostname):
        msg = f"Environment variable '{env_var_name}' contains an invalid hostname: '{hostname}'"
        raise InputError(msg, logger)

    if parsed.path and parsed.path not in ("", "/"):
        msg = f"Environment variable '{env_var_name}' should be a root URL without path components. Got path: '{parsed.path}'"
        raise InputError(msg, logger)

    return value.rstrip("/")
