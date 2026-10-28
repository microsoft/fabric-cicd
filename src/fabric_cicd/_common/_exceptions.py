# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Custom exceptions for the fabric-cicd package."""

from logging import Logger
from typing import Optional


class BaseCustomError(Exception):
    def __init__(self, message: str, logger: Logger, additional_info: Optional[str] = None) -> None:
        """
        Initialize the BaseCustomError.

        Args:
            message: The error message.
            logger: The logger instance.
            additional_info: Additional information about the error.
        """
        super().__init__(message)
        self.logger = logger
        self.additional_info = additional_info


class ParsingError(BaseCustomError):
    pass


class InputError(BaseCustomError):
    pass


class TokenError(BaseCustomError):
    pass


class InvokeError(BaseCustomError):
    pass


class ItemDependencyError(BaseCustomError):
    pass


class FileTypeError(BaseCustomError):
    pass


class ParameterFileError(BaseCustomError):
    pass


class FailedPublishedItemStatusError(BaseCustomError):
    pass


class PublishError(BaseCustomError):
    """Exception raised when one or more publish operations fail.

    Attributes:
        errors: List of (item_name, exception) tuples for all failed items.
        post_publish_error: The error raised by post_publish_all(), if it also failed.
    """

    def __init__(
        self, errors: list[tuple[str, Exception]], logger: Logger, post_publish_error: Optional[Exception] = None
    ) -> None:
        """Initialize with a list of (item_name, exception) tuples and an optional post-publish error."""
        self.errors = errors
        self.post_publish_error = post_publish_error
        failed_names = [name for name, _ in errors]
        message = f"Failed to publish {len(errors)} item(s): {failed_names}"
        if post_publish_error is not None:
            message += f". The post-publish step also failed: {post_publish_error}"

        additional_info_parts = []
        for item_name, exc in errors:
            additional_info_parts.append(f"\n--- {item_name} ---\n{exc!s}")
        post_publish_info = getattr(post_publish_error, "additional_info", None)
        if post_publish_info:
            additional_info_parts.append(f"\n--- post_publish_all ---\n{post_publish_info}")
        additional_info = "\n".join(additional_info_parts) if additional_info_parts else None

        super().__init__(message, logger, additional_info)
