from enum import Enum, unique


@unique
class Roles(Enum):
    #  These numeric constants CANNOT be changed once they have been used,
    #  because that would break backwards compatibility with the ledger
    # Also the numeric constants CANNOT collide with roles in dependent
    # components.
    TRUSTEE = "0"
    STEWARD = "2"

    def __str__(self):
        return self.name
