# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Utility functions for checking file types and versions."""

import logging
from importlib.metadata import version as lib_version
from pathlib import Path

import filetype
import requests
from colorama import Fore, Style
from packaging import version

from fabric_cicd._common._exceptions import FileTypeError

logger = logging.getLogger(__name__)


def check_version() -> None:
    """Check the current version of the fabric-cicd package and compare it with the latest version."""
    try:
        current_version = lib_version("fabric-cicd")
        response = requests.get("https://pypi.org/pypi/fabric-cicd/json")
        latest_version = response.json()["info"]["version"]
        if version.parse(current_version) < version.parse(latest_version):
            msg = (
                f"{Fore.BLUE}[notice]{Style.RESET_ALL} A new release of fabric-cicd is available: "
                f"{Fore.RED}{current_version}{Style.RESET_ALL} -> {Fore.GREEN}{latest_version}{Style.RESET_ALL}"
            )
            print(msg)
    except:
        pass


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
