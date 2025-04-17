# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Utility functions for checking file types and versions."""

import logging
import re
from pathlib import Path

import filetype
import requests
from packaging import version

import fabric_cicd.constants as constants
from fabric_cicd._common._color import Fore, Style
from fabric_cicd._common._exceptions import FileTypeError

logger = logging.getLogger(__name__)


def parse_changelog() -> dict[str, list[str]]:
    """Parse the changelog file and return a dictionary of versions with their changes."""
    content = None

    try:
        response = requests.get(
            "https://raw.githubusercontent.com/microsoft/fabric-cicd/refs/heads/main/src/fabric_cicd/changelog.md"
        )
        if response.status_code == 200:
            content = response.text
        else:
            logger.debug(f"Failed to fetch online changelog: HTTP {response.status_code}")
            return {}
    except Exception as e:
        logger.debug(f"Error fetching online changelog: {e}")
        return {}

    version_pattern = r"## Version (\d+\.\d+\.\d+).*?(?=## Version|\Z)"
    changelog_dict = {}

    for match in re.finditer(version_pattern, content, re.DOTALL):
        version_num = match.group(1)
        section_content = match.group(0)

        bullet_points = []
        for line in section_content.split("\n"):
            line = line.strip()
            if line.startswith("-"):
                bullet_points.append(line)

        changelog_dict[version_num] = bullet_points

    return changelog_dict


def check_version() -> None:
    """Check the current version of the fabric-cicd package and compare it with the latest version."""
    try:
        current_version = constants.VERSION
        response = requests.get("https://pypi.org/pypi/fabric-cicd/json")
        latest_version = response.json()["info"]["version"]

        if version.parse(current_version) < version.parse(latest_version):
            msg = (
                f"{Fore.BLUE}[notice]{Style.RESET_ALL} A new release of fabric-cicd is available: "
                f"{Fore.RED}{current_version}{Style.RESET_ALL} -> {Fore.GREEN}{latest_version}{Style.RESET_ALL}\n"
            )

            # Get changelog entries for versions between current and latest
            changelog_entries = parse_changelog()
            if changelog_entries:
                msg += f"{Fore.BLUE}[notice]{Style.RESET_ALL} What's new:\n\n"

                for ver_str, bullet_points in changelog_entries.items():
                    ver = version.parse(ver_str)
                    if version.parse(current_version) < ver <= version.parse(latest_version):
                        msg += f"{Fore.YELLOW}Version {ver_str}{Style.RESET_ALL}\n"
                        for point in bullet_points:
                            msg += f"  {point}\n"
                        msg += "\n"

            msg += (
                f"{Fore.BLUE}[notice]{Style.RESET_ALL} View the full changelog at: "
                f"{Fore.CYAN}https://microsoft.github.io/fabric-cicd/latest/changelog/{Style.RESET_ALL}"
            )

            print(msg)
    except Exception as e:
        # Silently handle errors, but log them if debug is needed
        logger.debug(f"Error checking version: {e}")
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
