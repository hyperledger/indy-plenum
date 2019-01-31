from collections import deque

from common.exceptions import LogicError
from plenum.server.replica_validator_enums import STASH_CATCH_UP, STASH_WATERMARKS, STASH_VIEW
from stp_core.common.log import getlogger


class StashDeque(deque):

    def append(self, x):
        if self.maxlen is None or len(self) < self.maxlen:
            super().append(x)


class ReplicaStasher:

    def __init__(self, replica, max_stashed=None) -> None:
        self.replica = replica
        if max_stashed is None:
            max_stashed = replica.config.REPLICA_STASH_LIMIT
        self._stashed_watermarks = StashDeque(maxlen=max_stashed)
        self._stashed_future_view = StashDeque(maxlen=max_stashed)
        self._stashed_catch_up = StashDeque(maxlen=max_stashed)
        self.logger = getlogger()

    @property
    def num_stashed_catchup(self):
        return len(self._stashed_catch_up)

    @property
    def num_stashed_future_view(self):
        return len(self._stashed_future_view)

    @property
    def num_stashed_watermarks(self):
        return len(self._stashed_watermarks)

    def stash(self, msg, reason):
        self.logger.trace("{} stash message '{}' "
                          "with reason {}".format(self.replica, msg, reason))
        if reason == STASH_CATCH_UP:
            self._stashed_catch_up.append(msg)
        elif reason == STASH_VIEW:
            self._stashed_future_view.append(msg)
        elif reason == STASH_WATERMARKS:
            self._stashed_watermarks.append(msg)
        else:
            raise LogicError("Unknown Stash Type '{}' "
                             "for message {}".format(reason, msg))

    def unstash_catchup(self):
        self.logger.info("{} unstash {} messages received in "
                         "catchup".format(self.replica, self.num_stashed_catchup))
        self._do_unstash(self._stashed_catch_up)

    def unstash_watermarks(self):
        self.logger.info("{} unstash {} out of watermarks "
                         "messages".format(self.replica, self.num_stashed_watermarks))
        self._do_unstash(self._stashed_watermarks)

    def unstash_future_view(self):
        self.logger.info("{} unstash {} messages from future "
                         "view".format(self.replica, self.num_stashed_future_view))
        self._do_unstash(self._stashed_future_view)

    def _do_unstash(self, stash):
        self.replica.inBox.extend(stash)
        stash.clear()
