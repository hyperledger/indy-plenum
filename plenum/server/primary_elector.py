import math
import random
import time
from collections import Counter
from functools import partial
from typing import Iterable, List, Sequence, Union

from plenum.common.constants import PRIMARY_SELECTION_PREFIX
from plenum.common.types import f
from plenum.common.messages.node_messages import Nomination, Reelection, Primary
from plenum.common.util import mostCommonElement
from stp_core.common.log import getlogger
from plenum.server import replica
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.router import Router, Route

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

        # Flag variable which indicates which replica has nominated for itself
        self.replicaNominatedForItself = None

        self.nominations = {}

        self.primaryDeclarations = {}

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
        # self.previous_master_primary = None

    @property
    def routes(self) -> Iterable[Route]:
        return [(Nomination, self.processNominate),
                (Primary, self.processPrimary),
                (Reelection, self.processReelection)]

    @property
    def hasPrimaryReplica(self) -> bool:
        """
        Return whether this node has a primary replica.
        """
        return any([r.isPrimary for r in self.replicas])

    # @property
    # def was_master_primary_in_prev_view(self):
    #     return self.previous_master_primary == self.name

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

    # overridden method of PrimaryDecider
    def decidePrimaries(self):
        self.scheduleElection()

    def scheduleElection(self):
        """
        Schedule election at some time in the future. Currently the election
        starts immediately.
        """
        self._schedule(self.startElection)

    def startElection(self):
        """
        Start the election process by nominating self as primary.
        """
        logger.debug("{} starting election".format(self))
        for r in self.replicas:
            self.prepareReplicaForElection(r)

        self.nominateItself()

    # overridden method of PrimaryDecider
    def start_election_for_instance(self, instance_id):
        self.prepareReplicaForElection(self.replicas[instance_id])
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
                logger.info("{} already has a primary replica".format(self.name))
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
            logger.info("{} does not have a primary, replicas {} are undecided, "
                        "choosing {} to nominate".format(self, undecideds, chosen))

            # A replica has nominated for itself, so set the flag
            self.replicaNominatedForItself = chosen
            self._schedule(partial(self.nominateReplica, chosen))
        else:
            logger.info("{} does not have a primary, but elections for all {} instances "
                        "have been decided".format(self, len(self.replicas)))

    def nominateReplica(self, instId):
        """
        Nominate the replica identified by `instId` on this node as primary.
        """
        replica = self.replicas[instId]
        if not self.didReplicaNominate(instId):
            self.nominations[instId][replica.name] = (
                replica.name, replica.last_ordered_3pc[1])
            logger.debug("{} nominating itself for instance {}".
                         format(replica, instId),
                         extra={"cli": "PLAIN", "tags": ["node-nomination"]})
            self.sendNomination(replica.name, instId,
                                self.viewNo, replica.last_ordered_3pc[1])
        else:
            logger.debug(
                "{} already nominated, so hanging back".format(replica))

    def _get_undecided_inst_id(self):
        undecideds = [i for i, r in enumerate(self.replicas)
                      if r.isPrimary is None]
        if 0 in undecideds and self.was_master_primary_in_prev_view:
            logger.info('{} was primary for master in previous view, '
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
        self.nominations[instId] = {}
        self.primaryDeclarations[instId] = {}
        self.scheduledPrimaryDecisions[instId] = None
        self.reElectionProposals[instId] = {}
        self.duplicateMsgs = {}

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
        if instId == 0 and replica.getNodeName(
                nom.name) == self.previous_master_primary:
            self.discard(
                nom, '{} got Nomination from {} for {} who was primary'
                ' of master in previous view too'. format(
                    self, sender, nom.name), logMethod=logger.debug)
            return False

        sndrRep = replica.generateName(sender, nom.instId)

        if not self.didReplicaNominate(instId):
            if instId not in self.nominations:
                self.setDefaults(instId)
            self.nominations[instId][replica.name] = (nom.name, nom.ordSeqNo)
            self.sendNomination(nom.name, nom.instId, nom.viewNo,
                                nom.ordSeqNo)
            logger.debug("{} nominating {} for instance {}".
                         format(replica, nom.name, nom.instId),
                         extra={"cli": "PLAIN", "tags": ["node-nomination"]})

        else:
            logger.debug("{} already nominated".format(replica.name))

        # Nodes should not be able to vote more than once
        if sndrRep not in self.nominations[instId]:
            self.nominations[instId][sndrRep] = (nom.name, nom.ordSeqNo)
            logger.debug("{} attempting to decide primary based on nomination "
                         "request: {} from {}".format(replica, nom, sndrRep))
            self._schedule(partial(self.decidePrimary, instId))
        else:
            self.discard(nom,
                         "already got nomination from {}".
                         format(sndrRep),
                         logger.debug)

            key = (Nomination.typename, instId, sndrRep)
            self.duplicateMsgs[key] = self.duplicateMsgs.get(key, 0) + 1

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
        if instId == 0 and replica.getNodeName(
                prim.name) == self.previous_master_primary:
            self.discard(prim, '{} got Primary from {} for {} who was primary'
                               ' of master in previous view too'.
                         format(self, sender, prim.name),
                         logMethod=logger.info)
            return

        sndrRep = replica.generateName(sender, prim.instId)

        # Nodes should not be able to declare `Primary` winner more than more
        if instId not in self.primaryDeclarations:
            self.setDefaults(instId)

        if sndrRep not in self.primaryDeclarations[instId]:
            self.primaryDeclarations[instId][sndrRep] = (prim.name,
                                                         prim.ordSeqNo)

            self.select_primary(instId, prim)
        else:
            self.discard(prim,
                         "already got primary declaration from {}".
                         format(sndrRep),
                         logger.debug)

            key = (Primary.typename, instId, sndrRep)
            self.duplicateMsgs[key] = self.duplicateMsgs.get(key, 0) + 1
            # If got more than one duplicate message then blacklist
            # if self.duplicateMsgs[key] > 1:
            #     self.send(BlacklistMsg(
            #         Suspicions.DUPLICATE_PRI_SENT.code, sender))

    def select_primary(self, inst_id: int, prim: Primary):
        # If got more than 2f+1 primary declarations then in a position to
        # decide whether it is the primary or not `2f + 1` declarations
        # are enough because even when all the `f` malicious nodes declare
        # a primary, we still have f+1 primary declarations from
        # non-malicious nodes. One more assumption is that all the non
        # malicious nodes vote for the the same primary

        # Find for which node there are maximum primary declarations.
        # Cant be a tie among 2 nodes since all the non malicious nodes
        # which would be greater than or equal to f+1 would vote for the
        # same node

        if replica.hasPrimary:
            logger.debug("{} Primary already selected; "
                         "ignoring PRIMARY msg"
                         .format(replica))
            return

        if self.hasPrimaryQuorum(inst_id):
            if replica.isPrimary is None:
                declarations = self.primaryDeclarations[inst_id]
                (primary, seqNo), freq = \
                    mostCommonElement(declarations.values())
                logger.display("{}{} selected primary {} for instance {} "
                               "(view {})"
                               .format(PRIMARY_SELECTION_PREFIX, replica,
                                       primary, inst_id, self.viewNo),
                               extra={"cli": "ANNOUNCE",
                                      "tags": ["node-election"]})
                logger.debug("{} selected primary on the basis of {}".
                             format(replica, declarations),
                             extra={"cli": False})

                # If the maximum primary declarations are for this node
                # then make it primary
                replica.primaryChanged(primary, seqNo)

                if inst_id == 0:
                    self.previous_master_primary = None

                # If this replica has nominated itself and since the
                # election is over, reset the flag
                if self.replicaNominatedForItself == inst_id:
                    self.replicaNominatedForItself = None

                self.node.primary_selected()

                self.scheduleElection()
            else:
                self.discard(prim,
                             "it already decided primary which is {}".
                             format(replica.primaryName),
                             logger.debug)
        else:
            logger.debug(
                "{} received {} but does it not have primary quorum "
                "yet".format(self.name, prim))

    def processReelection(self, reelection: Reelection, sender: str):
        """
        Process reelection requests sent by other nodes.
        If quorum is achieved, proceed with the reelection process.

        :param reelection: the reelection request
        :param sender: name of the  node from which the reelection was sent
        """
        logger.debug("{}'s elector started processing reelection msg".
                     format(self.name))
        # Check for election round number to discard any previous
        # reelection round message
        instId = reelection.instId
        replica = self.replicas[instId]
        sndrRep = replica.generateName(sender, reelection.instId)

        if instId not in self.reElectionProposals:
            self.setDefaults(instId)

        expectedRoundDiff = 0 if (replica.name in
                                  self.reElectionProposals[instId]) else 1
        expectedRound = self.reElectionRounds[instId] + expectedRoundDiff

        if not reelection.round == expectedRound:
            self.discard(reelection,
                         "reelection request from {} with round "
                         "number {} does not match expected {}".
                         format(sndrRep, reelection.round, expectedRound),
                         logger.debug)
            return

        if sndrRep not in self.reElectionProposals[instId]:
            self.reElectionProposals[instId][sndrRep] = [tuple(_) for _ in
                                                         reelection.tieAmong]

            # Check if got reelection messages from at least 2f + 1 nodes (1
            # more than max faulty nodes). Necessary because some nodes may
            # turn out to be malicious and send re-election frequently

            if self.hasReelectionQuorum(instId):
                logger.debug("{} achieved reelection quorum".
                             format(replica), extra={"cli": True})
                # Need to find the most frequent tie reported to avoid `tie`s
                # from malicious nodes. Since lists are not hashable so
                # converting each tie(a list of node names) to a tuple.
                ties = [tuple(t) for t in
                        self.reElectionProposals[instId].values()]
                tieAmong, freq = mostCommonElement(ties)

                self.setElectionDefaults(instId)

                if not self.hasPrimaryReplica and not self.was_master_primary_in_prev_view:
                    # There was a tie among this and some other node(s), so do a
                    # random wait
                    if replica.name in [_[0] for _ in tieAmong]:
                        # Try to nominate self after a random delay but dont block
                        # until that delay and because a nominate from another
                        # node might be sent
                        self._schedule(partial(self.nominateReplica, instId),
                                       random.randint(1, 3))
                    else:
                        # Now try to nominate self again as there is a
                        # reelection
                        self.nominateReplica(instId)
            else:
                logger.debug(
                    "{} does not have re-election quorum yet. "
                    "Got only {}".format(
                        replica, len(
                            self.reElectionProposals[instId])))
        else:
            self.discard(reelection,
                         "already got re-election proposal from {}".
                         format(sndrRep),
                         logger.debug)

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

    def hasPrimaryQuorum(self, instId: int) -> bool:
        """
        Are there at least `quorum` number of primary declarations received by
        this replica?

        :return: True if number of primary declarations is greater than quorum,
         False otherwise
        """
        pd = len(self.primaryDeclarations[instId])
        q = self.quorum
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
            logger.debug("{} has already sent a Primary: {}". format(
                replica, self.primaryDeclarations[instId][replica.name]))
            return

        if replica.name in self.reElectionProposals[instId]:
            logger.debug("{} has already sent a Re-Election for : {}". format(
                replica, self.reElectionProposals[instId][replica.name]))
            return

        if self.hasNominationQuorum(instId):
            logger.debug("{} has got nomination quorum now".
                         format(replica))
            primaryCandidates = self.getPrimaryCandidates(instId)

            # In case of one clear winner
            if len(primaryCandidates) == 1:
                (primaryName, seqNo), votes = primaryCandidates.pop()
                if self.hasNominationsFromAll(instId) or (
                        self.scheduledPrimaryDecisions[instId] is not None and
                        self.hasPrimaryDecisionTimerExpired(instId)):
                    logger.debug("{} has nominations from all so sending "
                                 "primary".format(replica))
                    self.sendPrimary(instId, primaryName, seqNo)
                else:
                    votesNeeded = math.ceil((self.nodeCount + 1) / 2.0)
                    if votes >= votesNeeded or (
                            self.scheduledPrimaryDecisions[instId] is not None and
                            self.hasPrimaryDecisionTimerExpired(instId)):
                        logger.debug("{} does not have nominations from "
                                     "all but has {} votes for {} so sending "
                                     "primary".
                                     format(replica, votes, primaryName))
                        self.sendPrimary(instId, primaryName, seqNo)
                        return
                    else:
                        logger.debug("{} has {} nominations for {}, but "
                                     "needs {}".format(replica, votes,
                                                       primaryName,
                                                       votesNeeded))
                        self.schedulePrimaryDecision(instId)
                        return
            else:
                logger.debug("{} has {} nominations. Attempting "
                             "reelection".
                             format(replica, self.nominations[instId]))
                if self.hasNominationsFromAll(instId) or (
                        self.scheduledPrimaryDecisions[instId] is not None and
                        self.hasPrimaryDecisionTimerExpired(instId)):
                    logger.debug(
                        "{} proposing re-election".format(replica),
                        extra={
                            "cli": True,
                            "tags": ['node-election']})
                    self.sendReelection(instId,
                                        [n[0] for n in primaryCandidates])
                else:
                    # Does not have enough nominations for a re-election so wait
                    # for some time to get nominations from remaining nodes
                    logger.debug("{} waiting for more nominations".
                                 format(replica))
                    self.schedulePrimaryDecision(instId)

        else:
            logger.debug("{} has not got nomination quorum yet".
                         format(replica))

    def sendNomination(self, name: str, instId: int, viewNo: int,
                       lastOrderedSeqNo: int):
        """
        Broadcast a nomination message with the given parameters.

        :param name: node name
        :param instId: instance id
        :param viewNo: view number
        """
        self.send(Nomination(name, instId, viewNo, lastOrderedSeqNo))

    def sendPrimary(self, instId: int, primaryName: str,
                    lastOrderedSeqNo: int):
        """
        Declare a primary and broadcast the message.

        :param instId: the instanceId to which the primary belongs
        :param primaryName: the name of the primary replica
        """
        replica = self.replicas[instId]
        self.primaryDeclarations[instId][replica.name] = (primaryName,
                                                          lastOrderedSeqNo)
        self.scheduledPrimaryDecisions[instId] = None
        logger.info("{} declaring primary as: {} on the basis of {}".
                    format(replica, primaryName, self.nominations[instId]))
        prim = Primary(primaryName, instId, self.viewNo,
                       lastOrderedSeqNo)
        self.send(prim)
        self.select_primary(instId, prim)

    def sendReelection(self, instId: int,
                       primaryCandidates: Sequence[str] = None) -> None:
        """
        Broadcast a Reelection message.

        :param primaryCandidates: the candidates for primary election of the
        election round for which reelection is being conducted
        """
        replica = self.replicas[instId]
        self.reElectionRounds[instId] += 1
        primaryCandidates = primaryCandidates if primaryCandidates \
            else self.getPrimaryCandidates(instId)
        self.reElectionProposals[instId][replica.name] = primaryCandidates
        self.scheduledPrimaryDecisions[instId] = None
        logger.debug("{} declaring reelection round {} for: {}".
                     format(replica.name,
                            self.reElectionRounds[instId],
                            primaryCandidates))
        self.send(
            Reelection(
                instId,
                self.reElectionRounds[instId],
                primaryCandidates,
                self.viewNo))

    def getPrimaryCandidates(self, instId: int):
        """
        Return the list of primary candidates, i.e. the candidates with the
        maximum number of votes
        """
        candidates = Counter(self.nominations[instId].values()).most_common()
        # Candidates with max no. of votes
        return [c for c in candidates if c[1] == candidates[0][1]]

    def schedulePrimaryDecision(self, instId: int):
        """
        Schedule a primary decision for the protocol instance specified by
        `instId` if not already done.
        """
        replica = self.replicas[instId]
        if not self.scheduledPrimaryDecisions[instId]:
            logger.debug("{} scheduling primary decision".format(replica))
            self.scheduledPrimaryDecisions[instId] = time.perf_counter()
            self._schedule(partial(self.decidePrimary, instId),
                           (1 * self.nodeCount))
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
        return (time.perf_counter() - self.scheduledPrimaryDecisions[instId]) \
            > (1 * self.nodeCount)

    def view_change_started(self, viewNo: int):
        """
        Actions to perform when a view change occurs.

        - Remove all pending messages which came for earlier views
        - Prepare for fresh elections
        - Schedule execution of any pending messages from the new view
        - Start elections again by nominating a random replica on this node

        :param viewNo: the new view number.
        """
        if super().view_change_started(viewNo):
            # Reset to defaults values for different data structures as new
            # elections would begin
            for r in self.replicas:
                self.setDefaults(r.instId)
            self.replicaNominatedForItself = None

            self.nominateRandomReplica()

        # if viewNo > self.viewNo:
        #     self.previous_master_primary = self.node.master_primary
        #
        #     self.viewNo = viewNo
        #
        #     for replica in self.replicas:
        #         replica.primaryName = None
        #
        #     # Reset to defaults values for different data structures as new
        #     # elections would begin
        #     for r in self.replicas:
        #         self.setDefaults(r.instId)
        #     self.replicaNominatedForItself = None
        #
        #     self.nominateRandomReplica()
        # else:
        #     logger.warning("Provided view no {} is not greater than the "
        #                    "current view no {}".format(viewNo, self.viewNo))

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
                                replica.last_ordered_3pc[1]))
        else:
            # If a primary for this instance has not been selected then send
            # nomination and primary declaration that this node made for the
            # instance with id `instId`
            if self.didReplicaNominate(instId):
                nm, seqNo = self.nominations[instId][replica.name]
                msgs.append(Nomination(nm, instId, self.viewNo, seqNo))
            if self.didReplicaDeclarePrimary(instId):
                nm, seqNo = self.primaryDeclarations[instId][replica.name]
                msgs.append(Primary(nm, instId, self.viewNo, seqNo))
        return msgs

    def get_msgs_for_lagged_nodes(self) -> List[Union[Nomination, Primary]]:
        """
        Get nomination and primary messages for instance with id `instId` that
        need to be sent to a node which has lagged behind (for example, a newly
        started node, a node that has crashed and recovered etc.)
        """
        msgs = []
        for instId in range(len(self.replicas)):
            msgs.extend(self.getElectionMsgsForInstance(instId))
        return msgs
