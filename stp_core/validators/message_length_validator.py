from plenum.common.exceptions import InvalidMessageExceedingSizeException


class MessageLenValidator:
    def __init__(self, max_allowed: int):
        self.max_allowed = max_allowed

    def validate(self, msg: bytes):
        has_len = len(msg)
        if not self.is_len_less_than_limit(has_len):
            raise InvalidMessageExceedingSizeException(
                self.max_allowed, has_len)

    def is_len_less_than_limit(self, ac_len):
        return ac_len <= self.max_allowed
