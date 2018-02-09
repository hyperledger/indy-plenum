import os

import plenum.config as plenum_config


def genesis_txn_file(transaction_file):
    return transaction_file + plenum_config.genesis_file_suffix


def genesis_txn_path(genesis_dir, transaction_file):
    return os.path.join(genesis_dir, genesis_txn_file(transaction_file))


def create_genesis_txn_init_ledger(data_dir, txn_file):
    from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
    initiator = GenesisTxnInitiatorFromFile(data_dir, txn_file)
    return initiator.create_initiator_ledger()
