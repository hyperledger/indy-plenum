import logging
from abc import ABC, abstractmethod
from typing import Optional, Tuple, Any, Callable, List

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import LedgerStatus
from stp_core.common.log import getlogger

logger = getlogger()


# TODO: Come up with a better name?
class CatchupDataProvider(ABC):
    @abstractmethod
    def node_name(self) -> str:
        pass

    @abstractmethod
    def ledgers(self) -> List[int]:
        pass

    @abstractmethod
    def ledger(self, ledger_id: int) -> Ledger:
        pass

    @abstractmethod
    # TODO: Delete when INDY-1946 gets implemented
    def three_phase_key_for_txn_seq_no(self, ledger_id: int, seq_no: int) -> Tuple[int, int]:
        pass

    @abstractmethod
    def update_txn_with_extra_data(self, txn: dict) -> dict:
        pass

    @abstractmethod
    def send_to(self, msg: Any, to: str, message_splitter: Optional[Callable] = None):
        pass

    @abstractmethod
    def send_to_nodes(self, msg: Any):
        pass

    @abstractmethod
    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        pass


def build_ledger_status(ledger_id: int, provider: CatchupDataProvider):
    ledger = provider.ledger(ledger_id)
    if ledger is None:
        raise ValueError("Cannot get ledger {} to build LEDGER_STATUS".format(ledger_id))

    # TODO: Delete when INDY-1946 gets implemented
    three_pc_key = provider.three_phase_key_for_txn_seq_no(ledger_id, ledger.size)
    view_no, pp_seq_no = three_pc_key if three_pc_key else (None, None)
    return LedgerStatus(ledger_id,
                        ledger.size,
                        view_no,
                        pp_seq_no,
                        ledger.root_hash,
                        CURRENT_PROTOCOL_VERSION)
