from plenum.common.constants import KeyValueStorageType


def get_config(config):
    config.auctionTransactionsFile = 'auction_transactions'
    config.auctionStateStorage = KeyValueStorageType.Leveldb
    config.auctionStateDbName = 'auction_state'
    return config
