# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Utility functions for checking file types and versions."""

import logging
import re
from pathlib import Path

import filetype
import requests
from colorama import Fore, Style
from packaging import version

import fabric_cicd.constants as constants
from fabric_cicd._common._exceptions import FileTypeError

logger = logging.getLogger(__name__)


def parse_changelog() -> dict[str, list[str]]:
    """Parse the changelog file and return a dictionary of versions with their changes."""
    content = None

    try:
        response = requests.get(constants.CHANGELOG_URL)
        if response.status_code == 200:
            content = response.text
        else:
            logger.debug(f"Failed to fetch online changelog: HTTP {response.status_code}")
            return {}
    except Exception as e:
        logger.debug(f"Error fetching online changelog: {e}")
        return {}

    changelog_dict = {}

    version_pattern = r"<h2 id=version-(\d+\d+\d+)[^>]*>Version ([0-9]+\.[0-9]+\.[0-9]+)"
    section_pattern = r"<h2 id=version-\d+[^>]*>Version [0-9]+\.[0-9]+\.[0-9]+.*?</h2>(.*?)(?=<h2 id=version|<h2 id=changelog|<footer)"

    for match in re.finditer(section_pattern, content, re.DOTALL):
        section_content = match.group(1)

        version_match = re.search(version_pattern, match.group(0))
        if not version_match:
            continue

        version_num = version_match.group(2)

        bullet_points = []
        li_pattern = r"<li>(.*?)</li>"
        for li_match in re.finditer(li_pattern, section_content, re.DOTALL):
            bullet_text = li_match.group(1).strip()
            link_pattern = r"<a href=([^>]+)>([^<]+)</a>"

            bullet_text = re.sub(link_pattern, r"\2 (\1)", bullet_text)
            bullet_text = "- " + bullet_text if not bullet_text.startswith("-") else bullet_text
            bullet_points.append(bullet_text)

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
                f"{Fore.CYAN}{constants.CHANGELOG_URL}{Style.RESET_ALL}"
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
