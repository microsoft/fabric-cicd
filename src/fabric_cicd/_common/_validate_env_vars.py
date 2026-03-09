# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions for validating environment variables and constants used by fabric-cicd."""

import logging
import os
import re
from urllib.parse import urlsplit

from fabric_cicd._common._exceptions import InputError

logger = logging.getLogger(__name__)

# Define a regular expression for valid hostnames
# Matches: any subdomain of [<word>]api.fabric.microsoft.com or [<word>]api.powerbi.com
_VALID_HOSTNAME_REGEX = re.compile(r"^([\w-]+\.)*[\w-]*api\.(fabric\.microsoft\.com|powerbi\.com)\Z", re.IGNORECASE)

# Constants that hold API URLs and require URL validation
_URL_CONSTANTS = {"DEFAULT_API_ROOT_URL", "FABRIC_API_ROOT_URL"}


def validate_api_url(url: str, label: str) -> str:
    """
    Validates an API URL string.
    Validates the value is non-empty, the scheme is https, the hostname matches
    allowed patterns, and no path components are present.

    Args:
        url: The URL string to validate.
        label: A human-readable label for error messages (e.g., env var name or config key).

    Returns:
        str: The validated URL with trailing slashes removed.
    """
    if not url.strip():
        msg = f"'{label}' must resolve to a non-empty string."
        raise InputError(msg, logger)

    # Parse the URL using urlsplit
    parsed = urlsplit(url)

    if parsed.scheme != "https":
        msg = f"Invalid or missing scheme in '{label}': '{url}'. URL must use HTTPS scheme."
        raise InputError(msg, logger)

    hostname = parsed.hostname or ""

    if not _VALID_HOSTNAME_REGEX.match(hostname):
        msg = f"'{label}' has invalid hostname: {hostname}"
        raise InputError(msg, logger)

    if parsed.path and parsed.path not in ("", "/"):
        msg = f"'{label}' should be a root URL without path components. Got path: '{parsed.path}'"
        raise InputError(msg, logger)

    return url.rstrip("/")


def validate_env_var_api_url(env_var_name: str, default_value: str) -> str:
    """
    Validates and returns the API URL from an environment variable.
    Validates the scheme is https and the hostname matches allowed patterns.

    Args:
        env_var_name: Name of the environment variable
        default_value: Default value if environment variable is not set (full URL with https://)

    Returns:
        str: The original validated API URL value, or the default if env var is not set.
    """
    value = os.environ.get(env_var_name, default_value)
    return validate_api_url(value, f"environment variable '{env_var_name}'")
