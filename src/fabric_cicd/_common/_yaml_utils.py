# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Utility functions for loading and validating YAML files."""

import logging
from pathlib import Path
from typing import Any

import yaml

from fabric_cicd._common._exceptions import InputError

logger = logging.getLogger(__name__)


def load_yaml_file(file_path: str, file_description: str = "YAML file") -> dict[str, Any]:
    """
    Load and parse a YAML file with comprehensive validation.

    Args:
        file_path: Path to the YAML file to load.
        file_description: Description of the file for error messages (e.g., "configuration file", "roles file").

    Returns:
        dict: Parsed YAML content as a dictionary.

    Raises:
        InputError: If file doesn't exist, is not a file, has invalid YAML syntax, or is not a dictionary.

    Examples:
        Basic usage
        >>> from fabric_cicd._common._yaml_utils import load_yaml_file
        >>> config = load_yaml_file("config.yml", "configuration file")

        With error handling
        >>> try:
        ...     config = load_yaml_file("config.yml")
        ... except InputError as e:
        ...     print(f"Failed to load config: {e}")
    """
    # Validate file path
    yaml_path = Path(file_path)
    if not yaml_path.exists():
        msg = f"{file_description.capitalize()} not found: {file_path}"
        raise InputError(msg, logger)

    if not yaml_path.is_file():
        msg = f"{file_description.capitalize()} path is not a file: {file_path}"
        raise InputError(msg, logger)

    # Load YAML content
    try:
        with Path.open(yaml_path, encoding="utf-8") as f:
            content = yaml.safe_load(f)
    except yaml.YAMLError as e:
        msg = f"Invalid YAML in {file_description}: {e}"
        raise InputError(msg, logger) from e
    except Exception as e:
        msg = f"Error reading {file_description}: {e}"
        raise InputError(msg, logger) from e

    # Validate structure
    if not isinstance(content, dict):
        msg = f"{file_description.capitalize()} must contain a dictionary"
        raise InputError(msg, logger)

    return content
