from enum import Enum


class Roles(Enum):
    TRUSTEE = "0"
    TGB = "1"
    STEWARD = "2"
    TRUST_ANCHOR = "3"

    def __str__(self):
        return self.name
