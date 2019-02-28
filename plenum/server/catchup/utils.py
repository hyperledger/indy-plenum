from abc import ABC, abstractmethod
from typing import Optional, Tuple, Any, Callable

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
    def update_txn_with_extra_data(self, txn: dict) -> dict:
        pass

    @abstractmethod
    def send_to(self, msg: Any, to: str, message_splitter: Optional[Callable] = None):
        pass

    @abstractmethod
    def send_to_nodes(self, msg: Any):
        pass


def build_ledger_status(ledger_id: int, ledger: Ledger, provider: CatchupDataProvider):
    view_no, pp_seq_no = (None, None)
    return LedgerStatus(ledger_id,
                        ledger.size,
                        view_no,
                        pp_seq_no,
                        ledger.root_hash,
                        CURRENT_PROTOCOL_VERSION)
