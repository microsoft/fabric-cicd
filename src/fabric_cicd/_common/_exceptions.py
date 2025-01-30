# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


class BaseCustomError(Exception):
    def __init__(self, message, logger, additional_info=None):
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


class UnhandledInvokeError(BaseCustomError):
    pass


class UnsupportedPrincipalTypeError(BaseCustomError):
    pass


class EnvironmentLibrariesNotFoundError(BaseCustomError):
    pass


class UnsupportedItemTypeError(BaseCustomError):
    pass


class LongRunningOperationError(BaseCustomError):
    pass


class EnvironmentValidationError(BaseCustomError):
    pass


class ExceededMaxRetryError(BaseCustomError):
    pass
