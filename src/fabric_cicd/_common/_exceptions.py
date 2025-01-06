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
