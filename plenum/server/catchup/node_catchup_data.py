import logging
from typing import List, Any, Optional, Callable, Iterable

from ledger.merkle_verifier import MerkleVerifier
from plenum.common.ledger import Ledger
from plenum.server.catchup.utils import CatchupDataProvider


class CatchupNodeDataProvider(CatchupDataProvider):
    def __init__(self, node):
        self._node = node

    def node_name(self) -> str:
        return self._node.name

    def all_nodes_names(self) -> List[str]:
        return self._node.allNodeNames

    def ledgers(self) -> List[int]:
        return self._node.ledger_ids

    def ledger(self, ledger_id: int) -> Ledger:
        info = self._ledger_info(ledger_id)
        return info.ledger if info is not None else None

    def verifier(self, ledger_id: int) -> MerkleVerifier:
        info = self._ledger_info(ledger_id)
        return info.verifier if info is not None else None

    def eligible_nodes(self) -> List[str]:
        return self._node.ledgerManager.nodes_to_request_txns_from

    def update_txn_with_extra_data(self, txn: dict) -> dict:
        return self._node.update_txn_with_extra_data(txn)

    def transform_txn_for_ledger(self, txn: dict) -> dict:
        return self._node.transform_txn_for_ledger(txn)

    def notify_catchup_start(self, ledger_id: int):
        if self._node.ledgerManager.preCatchupClbk:
            self._node.ledgerManager.preCatchupClbk(ledger_id)

        info = self._ledger_info(ledger_id)
        if info is not None and info.preCatchupStartClbk:
            info.preCatchupStartClbk()

    def notify_catchup_complete(self, ledger_id: int):
        if self._node.ledgerManager.postCatchupClbk:
            self._node.ledgerManager.postCatchupClbk(ledger_id)

        info = self._ledger_info(ledger_id)
        if info is not None and info.postCatchupCompleteClbk:
            info.postCatchupCompleteClbk()

    def notify_transaction_added_to_ledger(self, ledger_id: int, txn: dict):
        info = self._ledger_info(ledger_id)
        if info is not None and info.postTxnAddedToLedgerClbk:
            info.postTxnAddedToLedgerClbk(ledger_id, txn)

    def send_to(self, msg: Any, to: str, message_splitter: Optional[Callable] = None):
        if self._node.nodestack.hasRemote(to):
            self._node.sendToNodes(msg, [to], message_splitter)
        else:
            self._node.transmitToClient(msg, to)

    def send_to_nodes(self, msg: Any, nodes: Iterable[str] = None):
        self._node.sendToNodes(msg, nodes)

    def blacklist_node(self, node_name: str, reason: str):
        self._node.blacklistNode(node_name, reason)

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        self._node.discard(msg, reason, logMethod, cliOutput)

    def _ledger_info(self, ledger_id: int):
        return self._node.ledgerManager.ledgerRegistry.get(ledger_id)
