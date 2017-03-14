from enum import Enum


class Roles(Enum):
    #  These numeric constants CANNOT be changed once they have been used,
    #  because that would break backwards compatibility with the ledger
    TRUSTEE = "0"
    TGB = "1"
    STEWARD = "2"
    TRUST_ANCHOR = "3"

    def __str__(self):
        return self.name
