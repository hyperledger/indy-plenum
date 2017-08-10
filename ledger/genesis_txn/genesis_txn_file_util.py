import os

from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from plenum.common.config_util import getConfig

config = getConfig()


def genesis_txn_file(transaction_file):
    return transaction_file + config.genesis_file_suffix


def genesis_txn_path(base_dir, transaction_file):
    return os.path.join(base_dir, genesis_txn_file(transaction_file))


def create_genesis_txn_init_ledger(data_dir, txn_file):
    initiator = GenesisTxnInitiatorFromFile(data_dir, txn_file)
    return initiator.create_initiator_ledger()
