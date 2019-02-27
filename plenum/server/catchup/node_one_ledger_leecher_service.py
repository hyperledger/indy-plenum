from plenum.common.channel import RxChannel
from plenum.common.messages.node_messages import LedgerStatus, ConsistencyProof, CatchupRep
from plenum.server.catchup.utils import CatchupDataProvider


class NodeOneLedgerLeecherService:
    def __init__(self, ledger_id: int, input: RxChannel, provider: CatchupDataProvider):
        input.set_handler(LedgerStatus, self.process_ledger_status)
        input.set_handler(ConsistencyProof, self.process_consistency_proof)
        input.set_handler(CatchupRep, self.process_catchup_rep)

        self._ledger_id = ledger_id
        self._provider = provider

    def process_ledger_status(self, status: LedgerStatus, frm: str):
        pass

    def process_consistency_proof(self, cons_proof: ConsistencyProof, frm: str):
        pass

    def process_catchup_rep(self, rep: CatchupRep, frm: str):
        pass
