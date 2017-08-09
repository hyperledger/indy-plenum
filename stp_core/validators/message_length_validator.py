from plenum.common.exceptions import InvalidMessageExceedingSizeException


class MessageLenValidator:
    def __init__(self, max_allowed: int):
        self.max_allowed = max_allowed

    def validate(self, msg: bytes):
        has_len = len(msg)
        if has_len > self.max_allowed:
            raise InvalidMessageExceedingSizeException(
                self.max_allowed, has_len)
