"""
Some model objects used in Plenum protocol.
"""
import time
from typing import NamedTuple, Set, Optional, Any, Dict, Callable

from plenum.common.messages.node_messages import Prepare, Commit
from stp_core.common.log import getlogger

logger = getlogger()

ThreePhaseVotes = NamedTuple("ThreePhaseVotes", [
    ("voters", Set[str]),
    ("msg", Optional[Any])])


class TrackedMsgs(dict):

    def _get_key(self, msg):
        raise NotImplementedError

    def _new_vote_msg(self, msg):
        return ThreePhaseVotes(voters=set(), msg=msg)

    def _add_msg(self, msg, voter: str):
        key = self._get_key(msg)
        if key not in self:
            self[key] = self._new_vote_msg(msg)
        self[key].voters.add(voter)

    def _has_msg(self, msg) -> bool:
        key = self._get_key(msg)
        return key in self

    def _has_vote(self, msg, voter: str) -> bool:
        key = self._get_key(msg)
        return key in self and voter in self[key].voters

    def _has_enough_votes(self, msg, count) -> bool:
        key = self._get_key(msg)
        return self._has_msg(msg) and len(self[key].voters) >= count


class Prepares(TrackedMsgs):
    """
    Dictionary of received Prepare requests. Key of dictionary is a 2
    element tuple with elements viewNo, seqNo and value is a 2 element
    tuple containing request digest and set of sender node names(sender
    replica names in case of multiple protocol instances)
    (viewNo, seqNo) -> (digest, {senders})
    """

    def _get_key(self, prepare):
        return prepare.viewNo, prepare.ppSeqNo

    # noinspection PyMethodMayBeStatic
    def addVote(self, prepare: Prepare, voter: str) -> None:
        """
        Add the specified PREPARE to this replica's list of received
        PREPAREs.

        :param prepare: the PREPARE to add to the list
        :param voter: the name of the node who sent the PREPARE
        """
        self._add_msg(prepare, voter)

    # noinspection PyMethodMayBeStatic
    def hasPrepare(self, prepare: Prepare) -> bool:
        return super()._has_msg(prepare)

    # noinspection PyMethodMayBeStatic
    def hasPrepareFrom(self, prepare: Prepare, voter: str) -> bool:
        return super()._has_vote(prepare, voter)

    def hasQuorum(self, prepare: Prepare, quorum: int) -> bool:
        return self._has_enough_votes(prepare, quorum)


class Commits(TrackedMsgs):
    """
    Dictionary of received commit requests. Key of dictionary is a 2
    element tuple with elements viewNo, seqNo and value is a 2 element
    tuple containing request digest and set of sender node names(sender
    replica names in case of multiple protocol instances)
    """

    def _get_key(self, commit):
        return commit.viewNo, commit.ppSeqNo

    # noinspection PyMethodMayBeStatic
    def addVote(self, commit: Commit, voter: str) -> None:
        """
        Add the specified COMMIT to this replica's list of received
        COMMITs.

        :param commit: the COMMIT to add to the list
        :param voter: the name of the replica who sent the COMMIT
        """
        super()._add_msg(commit, voter)

    # noinspection PyMethodMayBeStatic
    def hasCommit(self, commit: Commit) -> bool:
        return super()._has_msg(commit)

    # noinspection PyMethodMayBeStatic
    def hasCommitFrom(self, commit: Commit, voter: str) -> bool:
        return super()._has_vote(commit, voter)

    def hasQuorum(self, commit: Commit, quorum: int) -> bool:
        return self._has_enough_votes(commit, quorum)


InstanceChangesVotes = NamedTuple("InstanceChangesVotes", [
    ("voters", Dict[str, int]),
    ("msg", Optional[Any])])


class InstanceChanges(TrackedMsgs):
    """
    Stores senders of received instance change requests. Key is the view
    no and and value is the set of senders
    Does not differentiate between reason for view change. Maybe it should,
    but the current assumption is that since a malicious node can raise
    different suspicions on different nodes, its ok to consider all suspicions
    that can trigger a view change as equal
    """

    def __init__(self, config, time_provider: Callable = time.perf_counter) -> None:
        self._outdated_ic_interval = \
            config.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL
        self._time_provider = time_provider
        super().__init__()

    def _new_vote_msg(self, msg):
        return InstanceChangesVotes(voters=dict(), msg=msg)

    def _get_key(self, msg):
        return msg if isinstance(msg, int) else msg.viewNo

    def add_vote(self, msg, voter: str):
        # This method can't use _add_message() because
        # the voters collection is a dict.
        key = self._get_key(msg)
        if key not in self:
            self[key] = self._new_vote_msg(msg)
        self[key].voters[voter] = self._time_provider()

    def has_view(self, view_no: int) -> bool:
        self._update_votes(view_no)
        return super()._has_msg(view_no)

    def has_inst_chng_from(self, view_no: int, voter: str) -> bool:
        self._update_votes(view_no)
        return super()._has_vote(view_no, voter)

    def has_quorum(self, view_no: int, quorum: int) -> bool:
        self._update_votes(view_no)
        return self._has_enough_votes(view_no, quorum)

    def _update_votes(self, view_no: int):
        if self._outdated_ic_interval <= 0 or view_no not in self:
            return
        for voter, vote_time in dict(self[view_no].voters).items():
            now = self._time_provider()
            if vote_time < now - self._outdated_ic_interval:
                logger.info("Discard InstanceChange from {} for ViewNo {} "
                            "because it is out of date (was received {}sec "
                            "ago)".format(voter, view_no, int(now - vote_time)))
                del self[view_no].voters[voter]
            if not self[view_no].voters:
                del self[view_no]
