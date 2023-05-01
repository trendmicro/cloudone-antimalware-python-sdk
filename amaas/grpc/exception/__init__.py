class RateLimitExceededError(Exception):
    """Raised by the SDK library to indicate http 429 too many request error."""
    def __init__(self, msg=None, *args, **kwargs):
        super().__init__(msg or self.__doc__, *args, **kwargs)
