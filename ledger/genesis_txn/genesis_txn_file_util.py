import os

from plenum.common.config_util import getConfig

config = getConfig()


def genesis_txn_file(transaction_file):
    return transaction_file + config.genesis_file_suffix


def genesis_txn_path(base_dir, transaction_file):
    return os.path.join(base_dir, genesis_txn_file(transaction_file))


def update_genesis_txn_file_name_if_outdated(base_dir, transaction_file):
    old_named_path = os.path.join(base_dir, transaction_file)
    new_named_path = os.path.join(base_dir, genesis_txn_file(transaction_file))
    if not os.path.exists(new_named_path) and os.path.isfile(old_named_path):
        os.rename(old_named_path, new_named_path)


def create_genesis_txn_init_ledger(data_dir, txn_file):
    from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
    initiator = GenesisTxnInitiatorFromFile(data_dir, txn_file)
    return initiator.create_initiator_ledger()
