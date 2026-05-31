# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Utility functions for checking file types and versions."""

import json
import logging
import re
from pathlib import Path

import filetype
import yaml

from fabric_cicd._common._exceptions import FileTypeError

logger = logging.getLogger(__name__)


def check_file_type(file_path: Path) -> str:
    """
    Check the type of the provided file.

    Args:
        file_path: The path to the file.
    """
    try:
        kind = filetype.guess(file_path)
    except Exception as e:
        msg = f"Error determining file type of {file_path}: {e}"
        FileTypeError(msg, logger)

    if kind is not None:
        if kind.mime.startswith("application/"):
            return "binary"
        if kind.mime.startswith("image/"):
            return "image"
    return "text"


def check_regex(regex: str) -> re.Pattern:
    """
    Check if a regex pattern is valid and returns the pattern.

    Args:
        regex: The regex pattern to match.
    """
    try:
        regex_pattern = re.compile(regex)
    except Exception as e:
        msg = f"An error occurred with the regex provided: {e}"
        raise ValueError(msg) from e
    return regex_pattern


def check_valid_json_content(content: str) -> bool:
    """
    Check if the given string content is valid JSON.

    Args:
        content: The string content to validate as JSON.

    Returns:
        bool: True if the content is valid JSON, False otherwise.
    """
    try:
        json.loads(content)
        return True
    except json.JSONDecodeError:
        return False


def check_valid_yaml_content(content: str) -> bool:
    """
    Check if the given string content is valid structured YAML (mapping or sequence).

    Args:
        content: The string content to validate as YAML.

    Returns:
        bool: True if the content parses as a YAML mapping or sequence, False otherwise.
    """
    try:
        result = yaml.safe_load(content)
        return isinstance(result, (dict, list))
    except yaml.YAMLError:
        return False


def check_tmdl_file(file_path: Path) -> bool:
    """
    Check if the file is a TMDL file by extension.

    Args:
        file_path: The file path to check.

    Returns:
        bool: True when the file path ends with .tmdl, otherwise False.
    """
    return str(file_path).endswith(".tmdl")
