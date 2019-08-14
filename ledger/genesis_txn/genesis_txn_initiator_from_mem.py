from typing import List, Dict

from ledger.genesis_txn.genesis_txn_initiator import GenesisTxnInitiator


class GenesisTxnInitiatorFromMem(GenesisTxnInitiator):
    def __init__(self, txns: List[Dict]):
        self._txns = txns

    def init_ledger_from_genesis_txn(self, ledger):
        for txn in self._txns:
            ledger.add(txn)

    def create_initiator_ledger(self):
        pass
