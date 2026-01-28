# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Cross-platform file locking."""

import sys
from pathlib import Path
from types import TracebackType
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


class FileLock:
    """File lock context manager."""

    def __init__(self, lock_file: str) -> None:
        self.lock_path = Path(f"{lock_file}.lock")
        self._lock_file: Optional[object] = None

    def __enter__(self) -> "FileLock":
        self._lock_file = self.lock_path.open("w")
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        if self._lock_file:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
        return False

    @staticmethod
    def run_with_lock(lock_file: str, func: Callable[[], T]) -> T:
        """
        Execute a function while holding an exclusive file lock.

        Args:
            lock_file: Path to the file to lock (a .lock suffix will be added)
            func: The function to execute while holding the lock

        Returns:
            The return value of the function
        """
        with FileLock(lock_file):
            return func()
