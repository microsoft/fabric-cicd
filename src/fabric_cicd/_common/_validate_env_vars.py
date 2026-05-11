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


# Regular expression for valid GUIDs with dashes
VALID_GUID_REGEX = r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"

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


def _get_fabric_fqdn_url(workspace_id: str) -> str:
    """
    Transform workspace ID to FQDN format for private-link-enabled Fabric workspaces.

    Args:
        workspace_id: The workspace ID string in standard GUID format with dashes
            (e.g., "f953f3da-c5f0-4e36-a644-c85933e35e2f").

    Returns:
        The FQDN URL string in the format:
        https://<workspace_id_no_dashes>.z<first_2_chars>.w.api.fabric.microsoft.com

    Examples:
        >>> url = _get_fabric_fqdn_url("f953f3da-c5f0-4e36-a644-c85933e35e2f")
        >>> url
        'https://f953f3dac5f04e36a644c85933e35e2f.zf9.w.api.fabric.microsoft.com'
    """
    if not re.match(VALID_GUID_REGEX, workspace_id):
        msg = f"workspace_id must be a valid GUID with dashes, got: '{workspace_id}'"
        raise ValueError(msg)
    no_dashes = workspace_id.replace("-", "")
    first_two = no_dashes[:2]
    return f"https://{no_dashes}.z{first_two}.w.api.fabric.microsoft.com"
