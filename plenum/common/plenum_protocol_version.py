from enum import Enum, unique


@unique
class PlenumProtocolVersion(Enum):
    #  These numeric constants CANNOT be changed once they have been used
    STATE_PROOF_SUPPORT = 1
    TXN_FORMAT_1_0_SUPPORT = 2

    def __str__(self):
        return self.name

    @staticmethod
    def has_value(value):
        try:
            PlenumProtocolVersion(value)
            return True
        except ValueError:
            return False
