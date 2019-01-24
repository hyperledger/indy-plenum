from stp_core.common.log import Logger

logger = Logger()


class Tracker:

    def __init__(self):
        self.un_committed = []

    def track(self, state_root, ledger_size):

        un_committed_state = ()

        if state_root != "":
            un_committed_state += (state_root,)
        else:
            raise logger.error("Empty state root given")

        if ledger_size > 0:
            un_committed_state += (ledger_size,)
        else:
            raise logger.error("Incorrect size of ledger given")

        self.un_committed.append(un_committed_state)

    def reject_batch(self):
        # what is the output of a one variable  array from this

        if len(self.un_committed) != 0:
            self.un_committed = self.un_committed[:-1]
        else:
            raise logger.error("No items to revert in Tracker")
