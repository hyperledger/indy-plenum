from plenum.common.transactions import Transactions


# DO NOT CHANGE ONCE CODE IS DEPLOYED ON THE LEDGER
# TODO: Might need a short hash with unique entropy, plugin name being input of hash.
PREFIX = '9999'


class DemoTransactions(Transactions):
    #  These numeric constants CANNOT be changed once they have been used,
    #  because that would break backwards compatibility with the ledger
    # Also the numeric constants CANNOT collide with other transactions hence a
    # prefix is used
    AUCTION_START = PREFIX + '0'
    AUCTION_END = PREFIX + '1'
    PLACE_BID = PREFIX + '2'
    GET_BAL = PREFIX + '3'
