class BaseCustomException(Exception):
    def __init__(self, message, logger, additional_info=None):
        super().__init__(message)
        self.logger = logger
        self.additional_info = additional_info


class ParsingError(BaseCustomException):
    pass


class InputError(BaseCustomException):
    pass


class TokenError(BaseCustomException):
    pass


class InvokeError(BaseCustomException):
    pass
