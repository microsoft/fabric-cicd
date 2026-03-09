# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Functions for validating environment variables used by fabric-cicd."""

import logging
import os
import re
from urllib.parse import urlsplit

from fabric_cicd._common._exceptions import InputError

logger = logging.getLogger(__name__)


# Define a regular expression for valid hostnames
# Matches: any subdomain of [<word>]api.fabric.microsoft.com or [<word>]api.powerbi.com
_VALID_HOSTNAME_REGEX = re.compile(r"^([\w-]+\.)*[\w-]*api\.(fabric\.microsoft\.com|powerbi\.com)\Z", re.IGNORECASE)


def validate_api_url_hostname(env_var_name: str, default_value: str) -> str:
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

    if not value.strip():
        msg = f"Environment variable '{env_var_name}' must resolve to a non-empty string."
        raise InputError(msg, logger)

    # Parse the URL using urlsplit
    parsed = urlsplit(value)

    if parsed.scheme != "https":
        msg = f"Invalid or missing scheme in environment variable {env_var_name}: '{value}'. URL must start with 'https://'"
        raise InputError(msg, logger)

    hostname = parsed.hostname or ""

    if not _VALID_HOSTNAME_REGEX.match(hostname):
        msg = f"Invalid hostname in environment variable {env_var_name}: {hostname}"
        raise InputError(msg, logger)

    if parsed.path and parsed.path not in ("", "/"):
        msg = f"Environment variable '{env_var_name}' should be a root URL without path components. Got path: '{parsed.path}'"
        raise InputError(msg, logger)

    return value.rstrip("/")
