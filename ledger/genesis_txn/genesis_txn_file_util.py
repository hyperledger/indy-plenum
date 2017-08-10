import os

from plenum.common.config_util import getConfig

config = getConfig()

def genesis_txn_file(transaction_file):
    return transaction_file + config.genesis_file_suffix


def genesis_txn_path(base_dir, transaction_file):
    return os.path.join(base_dir, genesis_txn_file(transaction_file))
