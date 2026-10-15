# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Owner-only file permission enforcement on POSIX systems."""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# True on Linux/macOS; False on Windows where POSIX permission bits are a no-op.
IS_POSIX = os.name != "nt"

# Owner read/write only (rw-------).
OWNER_ONLY_FILE_MODE = 0o600


def restricted_opener(path: str, flags: int) -> int:
    """Open a file with owner-only permissions (0o600).

    Intended for use as the opener argument to open() so that new files
    are created with restricted permissions from the start, eliminating
    any race window. Only effective for new files; use restrict_file()
    for pre-existing files.
    """
    return os.open(path, flags, OWNER_ONLY_FILE_MODE)


def restrict_file(path: str) -> None:
    """Set owner-only permissions (0o600) on an existing file.

    No-op on Windows or when the file does not exist.
    Logs at debug level on failure since the user cannot act on it.
    """
    if not IS_POSIX:
        return
    p = Path(path)
    if not p.exists():
        return
    try:
        p.chmod(OWNER_ONLY_FILE_MODE)
    except OSError as e:
        logger.debug("Failed to set permissions %o on %s: %s", OWNER_ONLY_FILE_MODE, path, e)
