from enum import Enum


class Transactions(Enum):
    def __str__(self):
        return self.name


class PlenumTransactions(Transactions):
    #  These numeric constants CANNOT be changed once they have been used,
    #  because that would break backwards compatibility with the ledger
    # Also the numeric constants CANNOT collide with transactions in dependent
    # components.
    NODE = "0"
    NYM = "1"
    GET_TXN = "3"
