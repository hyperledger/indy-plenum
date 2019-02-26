from plenum.common.channel import RxChannel
from plenum.common.messages.node_messages import LedgerStatus
from plenum.server.catchup.seeder_service import SeederService
from plenum.server.catchup.utils import CatchupDataProvider
from stp_core.common.log import getlogger

logger = getlogger()


class NodeSeederService(SeederService):
    def __init__(self, input: RxChannel, provider: CatchupDataProvider):
        SeederService.__init__(input, provider)

        input.set_handler(LedgerStatus, self.process_ledger_status)

    def process_ledger_status(self, status: LedgerStatus, frm: str):
        logger.info("{} received ledger status: {} from {}".format(self, status, frm))

        ledger_id, ledger = self._get_ledger_and_id(status)

        if ledger is None:
            logger.warning("{} discarding message {} from {} because it references invalid ledger".
                           format(self, status, frm))
            return

        if status.txnSeqNo < 0:
            logger.warning("{} discarding message {} from {} because it contains negative sequence number".
                           format(self, status, frm))
            return

        if status.txnSeqNo >= ledger.size:
            return

        try:
            cons_proof = self._build_consistency_proof(ledger_id, status.txnSeqNo, ledger.size)

            logger.info("{} sending consistency proof: {} to {}".format(self, cons_proof, frm))
            self._provider.send_to(cons_proof, frm)
        except ValueError as e:
            logger.warning("{} discarding message {} from {} because {}".
                           format(self, status, frm, e))
            return
