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
