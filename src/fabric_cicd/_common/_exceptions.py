class ParsingError(Exception):
    def __init__(self, message, logger, additional_info=None):
        super().__init__(message)
        self.logger = logger


class InputError(Exception):
    def __init__(self, message, logger, additional_info=None):
        super().__init__(message)
        self.logger = logger


class TokenError(Exception):
    def __init__(self, message, logger, additional_info=None):
        super().__init__(message)
        self.logger = logger


class InvokeError(Exception):
    def __init__(self, message, logger, additional_info=None):
        super().__init__(message)
        self.logger = logger
        self.additional_info = additional_info
