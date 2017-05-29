import math
import random
import time
from collections import Counter, deque
from collections import OrderedDict
from functools import partial
from operator import itemgetter
from typing import Sequence, Any, Union, List

from plenum.common.types import Nomination, Reelection, Primary, f
from plenum.common.util import mostCommonElement, get_strong_quorum, \
    checkIfMoreThanFSameItems, updateNamedTuple, get_weak_quorum
from stp_core.common.log import getlogger
from plenum.server import replica
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.router import Router


logger = getlogger()


# The elector should not blacklist nodes if it receives multiple nominations
# or primary or re-election messages, until there are roo many (over 50 maybe)
# duplicate messages. Consider a case where a node say Alpha, took part in
# election and election completed and soon after that Alpha crashed. Now Alpha
#  comes back up and receives Nominations and Primary. Now Alpha will react to
#  that and send Nominations or Primary, which will lead to it being
# blacklisted. Maybe Alpha should not react to Nomination or Primary it gets
# for elections it was not part of. Elections need to have round numbers.


class PrimaryElector(PrimaryDecider):
    """
    Responsible for managing the election of a primary for all instances for
    a particular Node. Each node has a PrimaryElector.
    """

    def __init__(self, node):
        super().__init__(node)

        # TODO: How does primary decider ensure that a node does not have a
        # primary while its catching up
        self.node = node

        # Flag variable which indicates which replica has nominated for itself
        self.replicaNominatedForItself = None

        self.nominations = {}

        self.primaryDeclarations = {}

        self.last_primary_sent = {}

        self.scheduledPrimaryDecisions = {}

        self.reElectionProposals = {}

        self.reElectionRounds = {}

        # # Tracks when election started for each instance, once
        # # `MaxElectionTimeoutFactor`*node_count elapses and no primary decided,
        # # re-start election
        # self.election_start_times = {}

        routerArgs = [(Nomination, self.processNominate),
                      (Primary, self.processPrimary),
                      (Reelection, self.processReelection)]
        self.inBoxRouter = Router(*routerArgs)

        # Keeps track of duplicate messages received. Used to blacklist if
        # nodes send more than 1 duplicate messages. Useful to blacklist
        # nodes. This number `1` is configurable. The reason 1 duplicate
        # message is tolerated is because sometimes when a node communicates
        # to an already lagged node, an extra NOMINATE or PRIMARY might be sent
        self.duplicateMsgs = {}   # Dict[Tuple, int]

        # Need to keep track of who was primary for the master protocol
        # instance for previous view, this variable only matters between
        # elections, the elector will set it before doing triggering new
        # election and will reset it after primary is decided for master
        # instance
        self.previous_master_primary = None

    def __repr__(self):
        return "{}".format(self.name)

    @property
    def hasPrimaryReplica(self) -> bool:
        """
        Return whether this node has a primary replica.
        """
        return any([r.isPrimary for r in self.replicas])

    @property
    def was_master_primary_in_prev_view(self):
        return self.previous_master_primary == self.name

    def setDefaults(self, instId: int):
        """
        Set the default values for elections for a replica.

        :param instId: instance id
        """
        logger.debug(
            "{} preparing replica with instId {}".format(self.name, instId))
        self.reElectionRounds[instId] = 0
        self.setElectionDefaults(instId)

    def prepareReplicaForElection(self, replica: 'replica.Replica'):
        """
        Prepare the replica state to get ready for elections.

        :param replica: the replica to prepare for elections
        """
        instId = replica.instId
        if instId not in self.nominations:
            self.setDefaults(instId)

    @staticmethod
    def convert_ledger_summary(ledger_summary):
        """
        Converting string keys (denoting ledger id) of ledger summary to integer
        """
        return {int(k): v for k, v in ledger_summary.items()}

    def filterMsgs(self, wrappedMsgs: deque) -> deque:
        """
        Filters messages by view number so that only the messages that have the
        current view number are retained.

        :param wrappedMsgs: the messages to filter
        """
        filtered = deque()
        while wrappedMsgs:
            wrappedMsg = wrappedMsgs.popleft()
            msg, sender = wrappedMsg
            if hasattr(msg, f.LEDGERS.nm) and msg.ledgers:
                # Since sending of messages over transport which in turns
                # converts the message to JSON, results in converting
                # integer keys of dictionary to string
                msg = updateNamedTuple(msg, **{f.LEDGERS.nm: self.convert_ledger_summary(msg.ledgers)})
                wrappedMsg = (msg, sender)
            if hasattr(msg, f.VIEW_NO.nm):
                reqViewNo = getattr(msg, f.VIEW_NO.nm)
                if reqViewNo == self.viewNo:
                    filtered.append(wrappedMsg)
                else:
                    self.discard(wrappedMsg,
                                 "its view no {} is less than the elector's {}"
                                 .format(reqViewNo, self.viewNo),
                                 logger.debug)
            else:
                
                filtered.append(wrappedMsg)

        return filtered

    def didReplicaNominate(self, instId: int):
        """
        Return whether this replica nominated a candidate for election

        :param instId: the instance id (used to identify the replica on this node)
        """
        return instId in self.nominations and \
            self.replicas[instId].name in self.nominations[instId]

    def didReplicaDeclarePrimary(self, instId: int):
        """
        Return whether this replica a candidate as primary for election

        :param instId: the instance id (used to identify the replica on this node)
        """
        return instId in self.primaryDeclarations and \
            self.replicas[instId].name in self.primaryDeclarations[instId]

    async def serviceQueues(self, limit=None):
        """
        Service at most `limit` messages from the inBox.

        :param limit: the maximum number of messages to service
        :return: the number of messages successfully processed
        """
        return await self.inBoxRouter.handleAll(self.filterMsgs(self.inBox),
                                                limit)

    @property
    def quorum(self) -> int:
        r"""
        Return the quorum of this RBFT system. Equal to :math:`2f + 1`.
        """
        return get_strong_quorum(f=self.f)

    def decidePrimaries(self):  # overridden method of PrimaryDecider
        self.scheduleElection()

    def scheduleElection(self):
        """
        Schedule election at some time in the future. Currently the election
        starts immediately.
        """
        self._schedule(self.startElection)

    def startElection(self):
        """
        Start the election process by nominating self as primary. Calling this
        will trigger the election for all instances, to trigger election for
        only one instance, use `start_election_for_instance`
        """
        logger.debug("{} starting election".format(self))
        for r in self.replicas:
            self.prepareReplicaForElection(r)

        self._schedule(self.nominateItself, random.random())

    def start_election_for_instance(self, inst_id):
        # Called when starting election for a particular protocol instance
        self.prepareReplicaForElection(self.replicas[inst_id])
        self._schedule(self.nominateItself, random.random())

    def nominateItself(self):
        """
        Actions to perform if this node hasn't nominated any of its replicas.
        """
        if self.replicaNominatedForItself is None:
            # If does not have a primary replica then nominate a replica
            if not self.hasPrimaryReplica:
                logger.debug(
                    "{} attempting to nominate a replica".format(self.name))
                self.nominateRandomReplica()
            else:
                logger.debug(
                    "{} already has a primary replica".format(self.name))
        else:
            logger.debug(
                "{} already has an election in progress".format(self.name))

    def nominateRandomReplica(self):
        """
        Randomly nominate one of the replicas on this node (for which elections
        aren't yet completed) as primary.
        """
        if not self.node.isParticipating:
            logger.debug("{} cannot nominate a replica yet since catching up"
                         .format(self))
            return

        undecideds, chosen = self._get_undecided_inst_id()
        if chosen is not None:
            logger.debug("{} does not have a primary, "
                         "replicas {} are undecided, "
                         "choosing {} to nominate".
                         format(self, undecideds, chosen))

            # A replica has nominated for itself, so set the flag
            self.replicaNominatedForItself = chosen
            self._schedule(partial(self.nominateReplica, chosen))
        else:
            logger.debug("{} does not have a primary, "
                         "but elections for all {} instances "
                         "have been decided".
                         format(self, len(self.replicas)))

    def nominateReplica(self, instId):
        """
        Nominate the replica identified by `instId` on this node as primary.
        """
        replica = self.replicas[instId]
        if not self.didReplicaNominate(instId):
            last_ordered_pp_seq_no = replica.lastOrderedPPSeqNo
            ledger_summary = replica.last_ordered_summary
            self.nominations[instId][replica.name] = (replica.name,
                                                      last_ordered_pp_seq_no,
                                                      ledger_summary)
            logger.info("{} nominating itself for instance {}".
                        format(replica, instId),
                        extra={"cli": "PLAIN", "tags": ["node-nomination"]})
            self.sendNomination(replica.name, instId, self.viewNo,
                                last_ordered_pp_seq_no, ledger_summary)
            # Since a replica might have received nominations but all
            # nominations might have been behind
            self._schedule(partial(self.decidePrimary, instId))
        else:
            logger.debug(
                "{} already nominated, so hanging back".format(replica))

    def _get_undecided_inst_id(self):
        undecideds = [i for i, r in enumerate(self.replicas)
                      if r.isPrimary is None]
        if 0 in undecideds and self.was_master_primary_in_prev_view:
            logger.debug('{} was primary for master in previous view, '
                         'so will not nominate master replica'.format(self))
            undecideds.remove(0)

        if undecideds:
            return undecideds, random.choice(undecideds)
        return None, None

    # noinspection PyAttributeOutsideInit
    def setElectionDefaults(self, instId):
        """
        Set defaults for parameters used in the election process.
        """
        self.nominations[instId] = OrderedDict()
        self.primaryDeclarations[instId] = OrderedDict()
        self.scheduledPrimaryDecisions[instId] = None
        self.reElectionProposals[instId] = OrderedDict()
        self.duplicateMsgs = {}
        self.last_primary_sent = {}

    def processNominate(self, nom: Nomination, sender: str):
        """
        Process a Nomination message.

        :param nom: the nomination message
        :param sender: sender address of the nomination
        """
        logger.debug("{} elector started processing nominate msg: {}".
                     format(self.name, nom))
        instId = nom.instId
        replica = self.replicas[instId]
        if instId == 0 and replica.getNodeName(nom.name) == self.previous_master_primary:
            self.discard(nom, '{} got Nomination from {} for {} who was primary'
                              ' of master in previous view too'.
                         format(self, sender, nom.name),
                         logMethod=logger.warning)
            return False

        sndrRep = replica.generateName(sender, nom.instId)
        if instId not in self.nominations:
            self.setDefaults(instId)

        # Nodes should not be able to vote more than once
        if sndrRep in self.nominations[instId]:
            self.discard(nom,
                         "already got nomination from {}".
                         format(sndrRep),
                         logger.warning)
            key = (Nomination.typename, instId, sndrRep)
            self.duplicateMsgs[key] = self.duplicateMsgs.get(key, 0) + 1
            return False

        self.nominations[instId][sndrRep] = (nom.name, nom.ordSeqNo, nom.ledgers)

        if replica.lastOrderedPPSeqNo <= nom.ordSeqNo:
            if not self.didReplicaNominate(instId):
                # Not using the last ordered seqno of the sender node
                # since it might be malicious
                last_ordered_pp_seq_no = replica.lastOrderedPPSeqNo
                ledger_summary = replica.last_ordered_summary
                self.nominations[instId][replica.name] = (nom.name,
                                                          last_ordered_pp_seq_no,
                                                          ledger_summary)
                self.sendNomination(nom.name, nom.instId, nom.viewNo,
                                    last_ordered_pp_seq_no, ledger_summary)
                logger.debug("{} nominating {} for instance {}".
                             format(replica, nom.name, nom.instId),
                             extra={"cli": "PLAIN", "tags": ["node-nomination"]})

            else:
                logger.debug("{} already nominated".format(replica.name))
        else:
            logger.debug('{} not accepting {} from {} as it has last ordered '
                         'seqno as {}'.format(replica, nom, sender,
                                              replica.lastOrderedPPSeqNo))


        logger.debug("{} attempting to decide primary based on nomination "
                     "request: {} from {}".format(replica, nom, sndrRep))
        self._schedule(partial(self.decidePrimary, instId))
            # If got more than one duplicate message then blacklist
            # if self.duplicateMsgs[key] > 1:
            #     self.send(BlacklistMsg(Suspicions.DUPLICATE_NOM_SENT.code, sender))

    def processPrimary(self, prim: Primary, sender: str) -> None:
        """
        Process a vote from a replica to select a particular replica as primary.
        Once 2f + 1 primary declarations have been received, decide on a
        primary replica.

        :param prim: a vote
        :param sender: the name of the node from which this message was sent
        """
        logger.debug("{}'s elector started processing primary msg from {} : {}"
                     .format(self.name, sender, prim))
        instId = prim.instId
        replica = self.replicas[instId]
        if instId == 0 and replica.getNodeName(prim.name) == self.previous_master_primary:
            self.discard(prim, '{} got Primary from {} for {} who was primary'
                               ' of master in previous view too'.
                         format(self, sender, prim.name),
                         logMethod=logger.warning)
            return

        sndrRep = replica.generateName(sender, prim.instId)

        # Nodes should not be able to declare `Primary` winner more than more
        if instId not in self.primaryDeclarations:
            self.setDefaults(instId)

        if sndrRep not in self.primaryDeclarations[instId]:
            self.primaryDeclarations[instId][sndrRep] = (prim.name,
                                                         prim.ordSeqNo,
                                                         prim.ledgers)

            # If got more than 2f+1 primary declarations then in a position to
            # decide whether it is the primary or not `2f + 1` declarations
            # are enough because even when all the `f` malicious nodes declare
            # a primary, we still have f+1 primary declarations from
            # non-malicious nodes. One assumption is that all the non
            # malicious nodes vote for the the same primary
            # Find for which node there are maximum primary declarations.
            # Cant be a tie among 2 nodes since all the non malicious nodes
            # which would be greater than or equal to f+1 would vote for the
            # same node

            if replica.isPrimary is not None:
                logger.debug(
                    "{} Primary already selected; ignoring PRIMARY msg".format(
                        replica))
                return

            # If sent a Re-election but more than f nodes send a Primary for
            # the same node then discard the re-election and send Primary
            if replica.isPrimary is None and \
                    self.hasPrimaryQuorum(instId, strong=False) and \
                    self.has_sent_reelection(instId):
                logger.debug('{} breaking re-election tie with {} from {}'.
                             format(self, prim, sender))
                self.sendPrimary(instId, prim.name, prim.ordSeqNo, prim.ledgers)

            if self.hasPrimaryQuorum(instId):
                if replica.isPrimary is None:
                    transformed = []
                    for n, o, s in self.primaryDeclarations[instId].values():
                        s = sorted(((k, tuple(v)) for k, v in s.items()),
                                   key=itemgetter(0))
                        s = tuple(s)
                        transformed.append((n, o, s))
                    primary, seqNo, ledger_summary = mostCommonElement(transformed)
                    logger.display("{} selected primary {} for instance {} "
                                   "(view {})".format(replica, primary,
                                                      instId, self.viewNo),
                                   extra={"cli": "ANNOUNCE",
                                          "tags": ["node-election"]})
                    logger.debug("{} selected primary on the basis of {}".
                                 format(replica,
                                        self.primaryDeclarations[instId]),
                                 extra={"cli": False})
                    # If the maximum primary declarations are for this node
                    # then make it primary
                    ledger_summary = dict(ledger_summary)
                    replica.primary_changed(primary, prim.viewNo, seqNo,
                                            ledger_summary)

                    if instId == 0:
                        self.previous_master_primary = None

                    # If for any reason, this could not get sufficient nominates
                    #  to send a primary but has got enough primary to decide,
                    # then send a primary now
                    if replica.name not in self.primaryDeclarations[instId]:
                        self.sendPrimary(instId, primary, seqNo, ledger_summary)

                    # If this replica has nominated itself and since the
                    # election is over, reset the flag
                    if self.replicaNominatedForItself == instId:
                        self.replicaNominatedForItself = None

                    self.node.primary_found()

                    # Nominate itself if not yet nominated for itself, needed in a scenario if each replica
                    # nominated itself for the same instance when election started.
                    self.scheduleElection()
                else:
                    self.discard(prim,
                                 "it already decided primary which is {}".
                                 format(replica.primaryName),
                                 logger.debug)
            else:
                logger.debug(
                    "{} received {} but it does not have primary quorum "
                    "yet".format(self.name, prim))
        else:
            self.discard(prim,
                         "already got primary declaration from {}".
                         format(sndrRep),
                         logger.warning)

            key = (Primary.typename, instId, sndrRep)
            self.duplicateMsgs[key] = self.duplicateMsgs.get(key, 0) + 1

    def processReelection(self, reelection: Reelection, sender: str):
        """
        Process reelection requests sent by other nodes.
        If quorum is achieved, proceed with the reelection process.

        :param reelection: the reelection request
        :param sender: name of the  node from which the reelection was sent
        """
        logger.debug("{} started processing reelection msg".
                     format(self.name))
        # Check for election round number to discard any previous
        # reelection round message
        inst_id = reelection.instId
        replica = self.replicas[inst_id]

        if inst_id in self.last_primary_sent:
            """
            Primary has already been sent for this instId.
            So send PRIMARY msg again.
            """
            self.resend_primary(inst_id, sender)
            return

        sndrRep = replica.generateName(sender, reelection.instId)

        if inst_id not in self.reElectionProposals:
            self.setDefaults(inst_id)

        expectedRoundDiff = 0 if (replica.name in
                                  self.reElectionProposals[inst_id]) else 1
        expectedRound = self.reElectionRounds[inst_id] + expectedRoundDiff

        if not reelection.round == expectedRound:
            self.discard(reelection,
                         "reelection request from {} with round "
                         "number {} does not match expected {}".
                         format(sndrRep, reelection.round, expectedRound),
                         logger.debug)
            return

        if sndrRep not in self.reElectionProposals[inst_id]:
            self.reElectionProposals[inst_id][sndrRep] = reelection.tieAmong

            # Check if got reelection messages from at least 2f + 1 nodes (1
            # more than max faulty nodes). Necessary because some nodes may
            # turn out to be malicious and send re-election frequently

            if self.hasReelectionQuorum(inst_id):
                # Need to find the most frequent tie reported to avoid `tie`s
                # from malicious nodes. Since lists are not hashable so
                # converting each tie(a list of node names) to a tuple.
                ties = [tuple(t) for t in
                        self.reElectionProposals[inst_id].values()]
                tieAmong = mostCommonElement(ties)

                if tieAmong:
                    logger.debug("{} achieved reelection quorum, tie between {}".
                                 format(replica, ', '.join(tieAmong)),
                                 extra={"cli": True})
                    self.setElectionDefaults(inst_id)

                    if not self.hasPrimaryReplica and not \
                            self.was_master_primary_in_prev_view:
                        # There was a tie among this and some other node(s), so do a
                        # random wait
                        if replica.name in tieAmong:
                            # Try to nominate self after a random delay but dont block
                            # until that delay and because a nominate from another
                            # node might be sent
                            self._schedule(partial(self.nominateReplica, inst_id),
                                           random.randint(1, 3))
                        else:
                            # Now try to nominate self again as there is a reelection
                            self.nominateReplica(inst_id)
                else:
                    logger.debug("{} achieved reelection quorum but no-tie, "
                                 "{} as not able to reach an acceptable "
                                 "state".format(replica, sndrRep))
                    self.setElectionDefaults(inst_id)
                    self.nominateReplica(inst_id)
            else:
                logger.debug("{} does not have re-election quorum yet. "
                             "Got only {}".format(replica,
                                                  len(self.reElectionProposals[inst_id])))
        else:
            self.discard(reelection,
                         "already got re-election proposal from {}".
                         format(sndrRep),
                         logger.warning)

    def hasReelectionQuorum(self, instId: int) -> bool:
        """
        Are there at least `quorum` number of reelection requests received by
        this replica?

        :return: True if number of reelection requests is greater than quorum,
        False otherwise
        """
        return len(self.reElectionProposals[instId]) >= self.quorum

    def hasNominationQuorum(self, instId: int) -> bool:
        """
        Are there at least `quorum` number of nominations received by this
        replica?

        :return: True if number of nominations is greater than quorum,
        False otherwise
        """
        return len(self.nominations[instId]) >= self.quorum

    def hasPrimaryQuorum(self, instId: int, strong=True) -> bool:
        """
        Are there at least `quorum` number of primary declarations received by
        this replica?

        :return: True if number of primary declarations is greater than quorum,
         False otherwise
        """
        pd = len(self.primaryDeclarations[instId])
        q = self.quorum if strong else get_weak_quorum(f=self.f)
        result = pd >= q
        if result:
            logger.trace("{} primary declarations {} meet required "
                         "quorum {} for instance id {}".
                         format(self.node.replicas[instId], pd, q, instId))
        return result

    def hasNominationsFromAll(self, instId: int) -> bool:
        """
        Did this replica receive nominations from all the replicas in the system?

        :return: True if this replica has received nominations from all replicas
        , False otherwise
        """
        return len(self.nominations[instId]) == self.nodeCount

    def decidePrimary(self, instId: int):
        """
        Decide which one among the nominated candidates can be a primary replica.
        Refer to the documentation on the election process for more details.
        """
        # Waiting for 2f+1 votes since at the most f nodes are malicious then
        # 2f+1 nodes have to be good. Not waiting for all nodes because some
        # nodes may turn out to be malicious and not vote at all

        replica = self.replicas[instId]

        if instId not in self.primaryDeclarations:
            self.primaryDeclarations[instId] = {}
        if instId not in self.reElectionProposals:
            self.reElectionProposals[instId] = {}
        if instId not in self.scheduledPrimaryDecisions:
            self.scheduledPrimaryDecisions[instId] = {}
        if instId not in self.reElectionRounds:
            self.reElectionRounds[instId] = 0

        if replica.name in self.primaryDeclarations[instId]:
            logger.debug("{} has already sent a Primary: {}".
                         format(replica,
                                self.primaryDeclarations[instId][replica.name]))
            return

        if replica.name in self.reElectionProposals[instId]:
            logger.debug("{} has already sent a Re-Election for : {}".
                         format(replica,
                                self.reElectionProposals[instId][replica.name]))
            return

        if not self.hasNominationQuorum(instId):
            # Should have nomination quorum since only `f` nodes are faulty
            logger.debug('{} does not have nomination quorum '
                         'so will try later'.format(replica))
            self._schedule(partial(self.decidePrimary, instId), 1)
            return

        # Primary is sent only when the largest last_ordered_seq has more than
        # f consistent Nominations (meaning if 2 Nominations have same last
        # ordered seq then their ledger summary should be same too)

        # If have nomination from all, then
        #   if there is an acceptable ordered state found, send primary else send re-election else
        # else wait for timer to expire

        if self.hasNominationsFromAll(instId) or (
                        self.scheduledPrimaryDecisions[instId] is not None and
                        self.hasPrimaryDecisionTimerExpired(instId)):
            logger.debug("{} has got all nominations".
                         format(replica))
            acceptable_state = self.get_acceptable_last_ordered_state(instId)
            if acceptable_state is not None:
                primary_candidates = self.get_primary_candidates(instId,
                                                                acceptable_state)
                if len(primary_candidates) == 1:
                    primaryName, votes = primary_candidates.pop()
                    self.sendPrimary(instId, primaryName, *acceptable_state)
                else:
                    self.sendReelection(instId,
                                        [n[0] for n in primary_candidates])
            else:
                logger.debug('{} cannot find acceptable state for instance {}, '
                             'sending re-election'.format(self, instId))
                self.sendReelection(instId)
        else:
            self.schedulePrimaryDecision(instId)

            # In case of one clear winner
            # if len(primary_candidates) == 1 and acceptable_state is not None:
            #     primaryName, votes = primary_candidates.pop()
            #     if self.hasNominationsFromAll(instId) or (
            #             self.scheduledPrimaryDecisions[instId] is not None and
            #             self.hasPrimaryDecisionTimerExpired(instId)):
            #         logger.debug("{} has nominations from all so sending "
            #                      "primary".format(replica))
            #         self.sendPrimary(instId, primaryName, seq_no)
            #     else:
            #         votesNeeded = math.ceil((self.nodeCount + 1) / 2.0)
            #         if votes >= votesNeeded or (
            #             self.scheduledPrimaryDecisions[instId] is not None and
            #             self.hasPrimaryDecisionTimerExpired(instId)):
            #             logger.debug("{} does not have nominations from "
            #                          "all but has {} votes for {} so sending "
            #                          "primary".
            #                          format(replica, votes, primaryName))
            #             self.sendPrimary(instId, primaryName, seq_no)
            #             return
            #         else:
            #             logger.debug("{} has {} nominations for {}, but "
            #                          "needs {}".format(replica, votes,
            #                                            primaryName,
            #                                            votesNeeded))
            #             self.schedulePrimaryDecision(instId)
            #             return
            # else:
            #     logger.debug("{} has {} nominations. Attempting "
            #                  "reelection".
            #                  format(replica, self.nominations[instId]))
            #     if self.hasNominationsFromAll(instId) or (
            #             self.scheduledPrimaryDecisions[instId] is not None and
            #             self.hasPrimaryDecisionTimerExpired(instId)):
            #         logger.info("{} proposing re-election".format(replica),
            #                     extra={"cli": True, "tags": ['node-election']})
            #         self.sendReelection(instId,
            #                             [n[0] for n in primary_candidates])
            #     else:
            #         # Does not have enough nominations for a re-election so wait
            #         # for some time to get nominations from remaining nodes
            #         logger.debug("{} waiting for more nominations".
            #                      format(replica))
            #         self.schedulePrimaryDecision(instId)

        # else:
        #     logger.debug("{} has not got nomination quorum yet".
        #                  format(replica))

    def sendNomination(self, name: str, instId: int, viewNo: int,
                       last_ordered_seq_no: int, ledgers):
        """
        Broadcast a nomination message with the given parameters.

        :param name: node name
        :param instId: instance id
        :param viewNo: view number
        """
        self.send(Nomination(name, instId, viewNo, last_ordered_seq_no, ledgers))

    def resend_primary(self, inst_id: int, sender):
        try:
            lps = self.last_primary_sent[inst_id]
            logger.debug('{} resending primary for instance {} to {}'.
                         format(self, inst_id, sender))
        except KeyError:
            raise RuntimeError("No previous Primary message sent for inst_id {}".
                               format(inst_id))
        self.send(lps, sender)

    def sendPrimary(self, inst_id: int, primary_name: str,
                    last_ordered_seq_no: int, ledgers):
        """
        Declare a primary and broadcast the message.

        :param inst_id: the instanceId to which the primary belongs
        :param primary_name: the name of the primary replica
        """
        replica = self.replicas[inst_id]
        self.primaryDeclarations[inst_id][replica.name] = (primary_name,
                                                           last_ordered_seq_no,
                                                           ledgers)
        self.scheduledPrimaryDecisions[inst_id] = None
        logger.debug("{} declaring primary as: {} on the basis of {}".
                     format(replica, primary_name, self.nominations[inst_id]))
        p = Primary(primary_name,
                    inst_id,
                    self.viewNo,
                    last_ordered_seq_no,
                    ledgers)
        self.last_primary_sent[inst_id] = p
        self.send(p)

    def sendReelection(self, instId: int,
                       primaryCandidates: Sequence[str] = None) -> None:
        """
        Broadcast a Reelection message.

        :param primaryCandidates: the candidates for primary election of the
        election round for which reelection is being conducted
        """
        replica = self.replicas[instId]
        self.reElectionRounds[instId] += 1
        primaryCandidates = primaryCandidates or []
        self.reElectionProposals[instId][replica.name] = primaryCandidates
        self.scheduledPrimaryDecisions[instId] = None
        logger.debug("{} declaring reelection round {} for: {}".
                     format(replica.name,
                            self.reElectionRounds[instId],
                            primaryCandidates))
        self.send(
            Reelection(instId, self.reElectionRounds[instId], self.viewNo,
                       primaryCandidates))

    def get_primary_candidates(self, inst_id: int, acceptable_state):
        """
        Return the list of primary candidates, i.e. the candidates with the
        maximum number of votes
        """
        acceptable_nominations = []
        for c, o, s in self.nominations[inst_id].values():
            if [o, s] == acceptable_state:
                acceptable_nominations.append(c)
        logger.debug('{} found acceptable nominations {} for instance {}'
                     .format(self, acceptable_nominations, inst_id))
        candidates = Counter(acceptable_nominations).most_common()
        # Candidates with max no. of votes
        return [c for c in candidates if c[1] == candidates[0][1]]

    def get_min_safe_last_ordered_pp_seq_no(self, inst_id):
        """
        Get minimum last ordered sequence number that has been nominated by
        greater than f replicas.
        """
        if len(self.nominations[inst_id]) > self.f:
            sorted_last_ordered = sorted([_[1] for _ in
                                          self.nominations[inst_id].values()],
                                         reverse=True)
            return sorted_last_ordered[-(self.f+1)]
        else:
            return None

    def get_acceptable_last_ordered_pp_seq_no(self, inst_id):
        """
        Get last ordered seqno for which there are >f nominations and
        there are <= f higher nominations. Returns None if cannot find such
        a number.
        """
        # Get frequency of each Nomination sorted on the basis of last
        # ordered pp seq no in descening order
        freq = sorted(Counter([_[1] for _ in
                               self.nominations[inst_id].values()]).most_common(),
                      key=itemgetter(0), reverse=True)

        if len(freq) == 1:
            acceptable = freq[0][0]
        elif len(freq) > 1:
            for i, (elem, count) in enumerate(freq):
                if count > self.f:
                    others = sum([c for (_, c) in freq[:i]])
                    if others <= self.f:
                        acceptable = elem
                        break
            else:
                return None
        else:
            return None
        return acceptable

    def get_acceptable_last_ordered_state(self, inst_id):
        """
        Get last ordered seqno for which there are >f nominations and
        there are <= f higher nominations. Returns None if cannot find such
        a number. If master instance then also compare ledger summaries
        :param inst_id:
        :return:
        """
        acceptable = self.get_acceptable_last_ordered_pp_seq_no(inst_id)
        if acceptable is None:
            return None
        r = [acceptable]

        if inst_id == 0:
            # Master instance
            summaries = []
            for _, last_ordered, summary in self.nominations[inst_id].values():
                if last_ordered == acceptable:
                    summaries.append(summary)
            acceptable_summary = checkIfMoreThanFSameItems(summaries, self.f)
            if not acceptable_summary:
                return None
            else:
                # Since the above method `checkIfMoreThanFSameItems` does json conversion
                acceptable_summary = self.convert_ledger_summary(acceptable_summary)
                r.append(acceptable_summary)
        else:
            r.append({})
        logger.debug('{} found acceptable last ordered state for instance {} '
                     'to be {}'.format(self, inst_id, r))
        return r

    def schedulePrimaryDecision(self, instId: int):
        """
        Schedule a primary decision for the protocol instance specified by
        `instId` if not already done.
        """
        replica = self.replicas[instId]
        if not self.scheduledPrimaryDecisions[instId]:
            delay = self.nodeCount
            logger.debug("{} scheduling primary decision in {} sec".
                         format(replica, delay))
            self.scheduledPrimaryDecisions[instId] = time.perf_counter()
            self._schedule(partial(self.decidePrimary, instId), delay)
        else:
            logger.debug("{} already scheduled primary decision".
                         format(replica))
            if self.hasPrimaryDecisionTimerExpired(instId):
                logger.debug("{} executing already scheduled primary "
                             "decision since timer expired".format(replica))
                self._schedule(partial(self.decidePrimary, instId))

    def hasPrimaryDecisionTimerExpired(self, instId: int) -> bool:
        """
        Check whether there has been a timeout while waiting for elections.

        :param instId: id of the instance for which elections are happening.
        """
        return (time.perf_counter() - self.scheduledPrimaryDecisions[instId]) > (1 * self.nodeCount)

    def has_sent_reelection(self, inst_id):
        replica = self.replicas[inst_id]
        return inst_id in self.reElectionProposals and replica.name in self.reElectionProposals[inst_id]

    def send(self, msg, rid: int=None):
        """
        Send a message to the node on which this replica resides.

        :param msg: the message to send
        :param rid: remote id to send to if not sending to all
        """
        logger.debug("{}'s elector sending {}".format(self.name, msg))
        self.outBox.append((msg, rid))

    def viewChanged(self, viewNo: int):
        """
        Actions to perform when a view change occurs.

        - Remove all pending messages which came for earlier views
        - Prepare for fresh elections
        - Schedule execution of any pending messages from the new view
        - Start elections again by nominating a random replica on this node

        :param viewNo: the new view number.
        """
        if viewNo > self.viewNo:
            self.previous_master_primary = self.node.master_primary

            self.viewNo = viewNo

            for replica in self.replicas:
                replica.primaryName = None

            # Reset to defaults values for different data structures as new
            # elections would begin
            for r in self.replicas:
                self.setDefaults(r.instId)
            self.replicaNominatedForItself = None
            self._schedule(self.nominateRandomReplica, random.random())
        else:
            logger.warning("Provided view no {} is not greater than the "
                           "current view no {}".format(viewNo, self.viewNo))

    def getElectionMsgsForInstance(self, instId: int) -> \
            Sequence[Union[Nomination, Primary]]:
        """
        Get nomination and primary messages for instance with id `instId`.
        """
        msgs = []
        replica = self.replicas[instId]
        # If a primary for this instance has been selected then send a
        # primary declaration for the selected primary
        if replica.isPrimary is not None:
            msgs.append(Primary(replica.primaryName, instId, self.viewNo,
                                replica.lastOrderedPPSeqNo,
                                replica.last_ordered_summary))
        else:
            # If a primary for this instance has not been selected then send
            # nomination and primary declaration that this node made for the
            # instance with id `instId`
            if self.didReplicaNominate(instId):
                nm, seqNo, summary = self.nominations[instId][replica.name]
                msgs.append(Nomination(nm, instId, self.viewNo, seqNo, summary))
            if self.didReplicaDeclarePrimary(instId):
                nm, seqNo, summary = self.primaryDeclarations[instId][replica.name]
                msgs.append(Primary(nm, instId, self.viewNo, seqNo, summary))
        return msgs

    def getElectionMsgsForLaggedNodes(self) -> \
            List[Union[Nomination, Primary]]:
        """
        Get nomination and primary messages for instance with id `instId` that
        need to be sent to a node which has lagged behind (for example, a newly
        started node, a node that has crashed and recovered etc.)
        """
        msgs = []
        for instId in range(len(self.replicas)):
            msgs.extend(self.getElectionMsgsForInstance(instId))
        return msgs
