from enum import Enum, unique


@unique
class PlenumTransactions(Enum):
    #  These numeric constants CANNOT be changed once they have been used,
    #  because that would break backwards compatibility with the ledger
    NODE = "0"
    NYM = "1"

    def __str__(self):
        return self.name
