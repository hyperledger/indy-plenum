from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler
from plenum.common.ledger import Ledger
from stp_core.common.log import Logger

logger = Logger()


class Tracker:

    def __init__(self):
        self.un_committed = []

    def track_uncommitted(self, state_root, ledger_size):

        un_committed_state = ()

        if state_root != "":
            un_committed_state = un_committed_state + (state_root,)
        else:
            raise logger.error("Empty state root given")

        if ledger_size > 0:
            un_committed_state = un_committed_state + (state_root,)
        else:
             raise logger.error("incorrect size of ledger given")

        self.un_committed.append(un_committed_state)

    def reject_batch(self):
        self.un_committed = self.un_committed[:-1]
