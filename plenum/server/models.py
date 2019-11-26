"""
Some model objects used in Plenum protocol.
"""
from typing import NamedTuple, Set, Optional, Any

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

    def _votes_count(self, msg) -> int:
        key = self._get_key(msg)
        if key not in self:
            return 0
        return len(self[key].voters)

    def _has_enough_votes(self, msg, count) -> bool:
        return self._votes_count(msg) >= count


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
