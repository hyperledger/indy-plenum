from plenum.common.transactions import Transactions


# DO NOT CHANGE ONCE CODE IS DEPLOYED ON THE LEDGER
# TODO: Might need a short hash with unique entropy, plugin name being input of hash.
PREFIX = '2022'


class DemoTransactions(Transactions):
    #  These numeric constants CANNOT be changed once they have been used,
    #  because that would break backwards compatibility with the ledger
    # Also the numeric constants CANNOT collide with other transactions hence a
    # prefix is used
    CREATE_DID = PREFIX + '0'
    CREATE_NETWORK_DID = PREFIX + '1'
    FETCH_DID = PREFIX + '2'
    UPDATE_DID = PREFIX + '3'
    UPDATE_NETWORK_DID = PREFIX + '4'
    DEACTIVATE_DID = PREFIX + '5'
