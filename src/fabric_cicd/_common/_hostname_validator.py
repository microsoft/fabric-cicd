# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import re
from urllib.parse import urlsplit

# Define a regular expression for valid hostnames
# Matches: any subdomain of [<word>]api.fabric.microsoft.com or [<word>]api.powerbi.com
VALID_HOSTNAME_REGEX = re.compile(r"^([\w-]+\.)*[\w-]*api\.(fabric\.microsoft\.com|powerbi\.com)$", re.IGNORECASE)


def validate_and_get_env_variable(env_var_name: str, default_value: str) -> str:
    """
    Validates and returns the URL from an environment variable.
    Validates the scheme is https and the hostname matches allowed patterns.

    Args:
        env_var_name (str): Name of the environment variable
        default_value (str): Default value if environment variable is not set (full URL with https://)

    Returns:
        str: The original validated URL value
    """
    value = os.environ.get(env_var_name, default_value)

    # Parse the URL using urlsplit
    parsed = urlsplit(value)

    if parsed.scheme != "https":
        msg = f"Invalid or missing scheme in environment variable {env_var_name}: '{value}'. URL must start with 'https://'"
        raise ValueError(msg)

    hostname = parsed.hostname or ""

    if not VALID_HOSTNAME_REGEX.match(hostname):
        msg = f"Invalid hostname in environment variable {env_var_name}: {hostname}"
        raise ValueError(msg)

    return value
