import logging
import sys
import time
from collections import Callable
from collections import deque
from typing import Any, Set, Optional
from typing import Dict, NamedTuple
from typing import Mapping
from typing import Tuple

from raet.raeting import AutoMode
from raet.road.estating import RemoteEstate, LocalEstate
from raet.road.keeping import RoadKeep
from raet.road.stacking import RoadStack
from raet.road.transacting import Joiner, Allower

from plenum.client.signer import Signer
from plenum.common.exceptions import RemoteNotFound
from plenum.common.ratchet import Ratchet
from plenum.common.request_types import Request, Batch, TaggedTupleBase
from plenum.common.util import error, distributedConnectionMap, \
    MessageProcessor, getlogger, checkPortAvailable

logger = getlogger()

HA = NamedTuple("HA", [
    ("host", str),
    ("port", int)])

# this overrides the defaults
Joiner.RedoTimeoutMin = 1.0
Joiner.RedoTimeoutMax = 2.0

Allower.RedoTimeoutMin = 1.0
Allower.RedoTimeoutMax = 2.0


class Stack(RoadStack):
    def __init__(self, *args, **kwargs):
        keep = RoadKeep(basedirpath=kwargs.get('basedirpath'),
                 stackname=kwargs['name'],
                 auto=kwargs.get('auto'),
                 baseroledirpath=kwargs.get('basedirpath'))
        kwargs['keep'] = keep
        localRoleData = keep.loadLocalRoleData()
        # local = LocalEstate(name=kwargs['name'],
        #             ha=kwargs['ha'],
        #             sigkey=localRoleData['sighex'],
        #             prikey=localRoleData['prihex'])
        # kwargs['local'] = local
        kwargs['sigkey'] = localRoleData['sighex']
        kwargs['prikey'] = localRoleData['prihex']
        super().__init__(*args, **kwargs)
        self.created = time.perf_counter()
        self.coro = self._raetcoro()
        self.msgHandler = None  # type: Callable

    async def service(self, limit=None) -> int:
        """
        Service `limit` number of received messages in this stack.

        :param limit: the maximum number of messages to be processed. If None,
        processes all of the messages in rxMsgs.
        :return: the number of messages processed.
        """
        pracLimit = limit if limit else sys.maxsize
        x = next(self.coro)
        if x > 0:
            for x in range(pracLimit):
                try:
                    self.msgHandler(self.rxMsgs.popleft())
                except IndexError:
                    break
        return x

    @property
    def age(self):
        """
        Returns the time elapsed since this stack was created
        """
        return time.perf_counter() - self.created

    def _raetcoro(self):
        """
        Generator to service all messages.
        Yields the length of rxMsgs queue of this stack.
        """
        while True:
            try:
                self._serviceStack(self.age)
                l = len(self.rxMsgs)
            except Exception as ex:
                if isinstance(ex, OSError) and \
                        len(ex.args) > 0 and \
                        ex.args[0] == 22:
                    logger.error("Error servicing stack: {}. This could be "
                                 "due to binding to an internal network "
                                 "and trying to route to an external one.".
                                 format(ex), extra={'cli': 'WARNING'})
                else:
                    logger.error("Error servicing stack: {} {}".
                                 format(ex, ex.args),
                                 extra={'cli': 'WARNING'})
                l = 0
            yield l

    def _serviceStack(self, age):
        """
        Update stacks clock and service all tx and rx messages.

        :param age: update timestamp of this RoadStack to this value
        """
        self.updateStamp(age)
        self.serviceAll()

    def updateStamp(self, age=None):
        """
        Change the timestamp of this stack's raet store.

        :param age: the timestamp will be set to this value
        """
        self.store.changeStamp(age if age else self.age)

    def close(self):
        """
        Close the UDP socket of this stack's server.
        """
        self.server.close()  # close the UDP socket

    def connecteds(self) -> Set[str]:
        """
        Return the names of the nodes this node is connected to.
        """
        return {r.name for r in self.remotes.values()
                if self.isRemoteConnected(r)}

    @staticmethod
    def isRemoteConnected(r: RemoteEstate) -> bool:
        """
        A node is considered to be connected if it is joined, allowed and alived.

        :param r: the remote to check
        """
        return r.joined and r.allowed and r.alived

    def getRemote(self, name: str) -> RemoteEstate:
        """
        Find the remote by name.

        :param name: the name of the remote to find
        :raises: RemoteNotFound
        """
        try:
            return next(r for r in self.remotes.values()
                        if r.name == name)
        except StopIteration:
            raise RemoteNotFound(name)

    def send(self, msg: Any, remoteName: str):
        """
        Transmit the specified message to the remote specified by `remoteName`.

        :param msg: a message
        :param remoteName: the name of the remote
        """
        rid = self.getRemote(remoteName).uid
        self.transmit(msg, rid)

    @classmethod
    def newStack(cls, stack):
        """
        Create a new instance of the given RoadStackClass

        :param stack: a dictionary of Roadstack constructor arguments.
        :return: the new instance of stack created.
        """
        checkPortAvailable(stack['ha'])
        stk = cls(**stack)
        if stk.ha[1] != stack['ha'].port:
            error("the stack port number has changed, likely due to "
                  "information in the keep")
        logger.info("stack {} starting at {} in {} mode"
                    .format(stk.name, stk.ha, stk.keep.auto.name),
                    extra={"cli": False})
        return stk


class ClientStacked:
    """
    Behaviors that provides Client Stack functionality to Nodes
    """
    def __init__(self, stackParams: dict):
        self.clientStackParams = stackParams
        self.clientstack = None  # type: RoadStack

    def startClientstack(self):
        self.clientstack = self.stackType().newStack(self.clientStackParams)
        self.clientstack.msgHandler = self.handleOneClientMsg

    def handleOneClientMsg(self, wrappedMsg):
        raise NotImplementedError("{} must implement this method".format(self))

    def transmitToClient(self, msg: Any, remoteName: str):
        """
        Transmit the specified message to the remote client specified by `remoteName`.

        :param msg: a message
        :param remoteName: the name of the remote
        """
        # At this time, nodes are not signing messages to clients, beyond what
        # happens inherently with RAET
        payload = self.prepForSending(msg)
        try:
            self.clientstack.send(payload, remoteName)
        except Exception as ex:
            logger.error("Unable to send message to client {}; Exception: {}"
                         .format(remoteName, ex.__repr__()))


class Batched(MessageProcessor):
    """
    A mixin to allow batching of requests to be send to remotes.
    """

    def __init__(self):
        """
        :param self: 'NodeStacked'
        """
        self.outBoxes = {}  # type: Dict[int, deque]

    def _enqueue(self, msg: Any, rid: int, signer: Signer) -> None:
        """
        Enqueue the message into the remote's queue.

        :param msg: the message to enqueue
        :param rid: the id of the remote node
        """
        payload = self.prepForSending(msg, signer)
        if rid not in self.outBoxes:
            self.outBoxes[rid] = deque()
        self.outBoxes[rid].append(payload)

    def _enqueueIntoAllRemotes(self, msg: Any, signer: Signer) -> None:
        """
        Enqueue the specified message into all the remotes in the nodestack.

        :param msg: the message to enqueue
        """
        for rid in self.nodestack.remotes.keys():
            self._enqueue(msg, rid, signer)

    def send(self, msg: Any, *rids: int, signer: Signer=None) -> None:
        """
        Enqueue the given message into the outBoxes of the specified remotes
         or into the outBoxes of all the remotes if rids is None

        :param msg: the message to enqueue
        :param rids: ids of the remotes to whose outBoxes
         this message must be enqueued
        """
        if rids:
            for r in rids:
                self._enqueue(msg, r, signer)
        else:
            self._enqueueIntoAllRemotes(msg, signer)

    def flushOutBoxes(self) -> None:
        """
        Clear the outBoxes and transmit batched messages to remotes.
        """
        removedRemotes = []
        for rid, msgs in self.outBoxes.items():
            try:
                dest = self.nodestack.remotes[rid].name
            except KeyError:
                removedRemotes.append(rid)
                continue
            if msgs:
                if len(msgs) == 1:
                    msg = msgs.popleft()
                    self.nodestack.transmit(msg, rid)
                    logger.trace("{} sending msg {} to {}".format(self, msg, dest))
                else:
                    logger.debug("{} batching {} msgs to {} into one transmission".
                                 format(self, len(msgs), dest))
                    logger.trace("    messages: {}".format(msgs))
                    batch = Batch([], None)
                    while msgs:
                        batch.messages.append(msgs.popleft())
                    # don't need to sign the batch, when the composed msgs are
                    # signed
                    payload = self.prepForSending(batch)
                    self.nodestack.transmit(payload, rid)
        for rid in removedRemotes:
            logger.warning("{} rid {} has been removed".format(self, rid),
                           extra={"cli": False})
            msgs = self.outBoxes[rid]
            if msgs:
                self.discard(msgs, "rid {} no longer available".format(rid))
            del self.outBoxes[rid]


class NodeStacked(Batched):
    """
    Behaviors that provides Node Stack functionality to Nodes and Clients
    """

    localips = ['127.0.0.1', '0.0.0.0']

    def __init__(self, stackParams: dict, nodeReg: Dict[str, HA]):
        super().__init__()
        self._name = stackParams["name"]
        # self.bootstrapped = False

        self.nodeStackParams = stackParams
        self.nodestack = None  # type: Stack

        self.lastcheck = {}  # type: Dict[int, Tuple[int, float]]
        self.ratchet = Ratchet(a=8, b=0.198, c=-4, base=8, peak=3600)
        self.nodeReg = nodeReg

        # holds the last time we checked remotes
        self.nextCheck = 0

        # courteous bi-directional joins
        self.connectNicelyUntil = None

        self._conns = set()  # type: Set[str]

        self.reconnectToMissingIn = 6
        self.reconnectToDisconnectedIn = 6

    def __repr__(self):
        return self.name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def isKeySharing(self):
        return self.nodestack.keep.auto != AutoMode.never

    @property
    def conns(self) -> Set[str]:
        """
        Get the connections of this node.

        :return: set of names of the connected nodes
        """
        return self._conns

    @conns.setter
    def conns(self, value: Set[str]) -> None:
        """
        Updates the connection count of this node if not already done.
        """
        if not self._conns == value:
            old = self._conns
            self._conns = value
            ins = value - old
            outs = old - value
            logger.debug("{}'s connection changed from {} to {}".format(self,
                                                                        old,
                                                                        value))
            self._connsChanged(ins, outs)

    def checkConns(self):
        """
        Evaluate the connected nodes
        """
        self.conns = self.nodestack.connecteds()

    def _connsChanged(self, ins: Set[str], outs: Set[str]) -> None:
        """
        A series of operations to perform once a connection count has changed.

        - Set f to max number of failures this system can handle.
        - Set status to one of started, started_hungry or starting depending on
            the number of protocol instances.
        - Check protocol instances. See `checkProtocolInstaces()`

        :param ins: new nodes connected
        :param outs: nodes no longer connected
        """
        for o in outs:
            logger.info("{} disconnected from {}".format(self, o),
                        extra={"cli": "IMPORTANT"})
        for i in ins:
            logger.info("{} now connected to {}".format(self, i),
                        extra={"cli": "IMPORTANT"})

            # remove remotes for same ha when a connection is made
            remote = self.nodestack.getRemote(i)
            others = [r for r in self.nodestack.remotes.values()
                      if r.ha == remote.ha and r.name != i]
            for o in others:
                logger.debug("{} removing other remote".format(self))
                self.nodestack.removeRemote(o)

        self.onConnsChanged(ins, outs)

    def onConnsChanged(self, ins: Set[str], outs: Set[str]):
        """
        Subclasses can override
        """
        pass

    def notConnectedNodes(self) -> Set[str]:
        """
        Returns the names of nodes in the registry this node is NOT connected
        to.
        """
        return set(self.nodeReg.keys()) - self.conns

    def startNodestack(self):
        self.nodestack = self.stackType().newStack(self.nodeStackParams)
        logger.info("{} listening for other nodes at {}:{}".
                    format(self, *self.nodestack.ha),
                    extra={"cli": "LOW_STATUS"})
        self.nodestack.msgHandler = self.handleOneNodeMsg

        if self.nodestack.name in self.nodeReg:
            # remove this node's registation from the Node Registry
            # (no need to connect to itself)
            del self.nodeReg[self.nodestack.name]

    def connect(self, name, rid: Optional[int]=None) -> Optional[int]:
        """
        Connect to the node specified by name.

        :param name: name of the node to connect to
        :type name: str or (HA, tuple)
        :return: the uid of the remote estate, or None if a connect is not
            attempted
        """
        # if not self.isKeySharing:
        #     logging.debug("{} skipping join with {} because not key sharing".
        #                   format(self, name))
        #     return None
        if rid:
            remote = self.nodestack.remotes[rid]
        else:
            if isinstance(name, (HA, tuple)):
                node_ha = name
            elif isinstance(name, str):
                node_ha = self.nodeReg[name]
            else:
                raise AttributeError()

            remote = RemoteEstate(stack=self.nodestack,
                                  ha=node_ha)
            self.nodestack.addRemote(remote)
        # updates the store time so the join timer is accurate
        self.nodestack.updateStamp()
        self.nodestack.join(uid=remote.uid, cascade=True, timeout=30)
        logger.info("{} looking for {} at {}:{}".
                    format(self.name, name or remote.name, *remote.ha),
                    extra={"cli": "PLAIN"})
        return remote.uid

    def sign(self, msg: Dict, signer: Signer) -> Dict:
        """
        No signing is implemented in NodeStacked. Returns the msg as it is.

        :param msg: the message to sign
        """
        return msg  # don't sign by default

    def prepForSending(self, msg: Dict, signer: Signer=None) -> Dict:
        """
        Return a dictionary form of the message

        :param msg: the message to be sent
        :raises: ValueError if msg cannot be converted to an appropriate format
            for transmission
        """
        if isinstance(msg, TaggedTupleBase):
            tmsg = msg.melted()
        elif isinstance(msg, Request):
            tmsg = msg.__getstate__()
        elif hasattr(msg, "_asdict"):
            tmsg = dict(msg._asdict())
        elif hasattr(msg, "__dict__"):
            tmsg = dict(msg.__dict__)
        else:
            raise ValueError("Message cannot be converted to an appropriate "
                             "format for transmission")
        if signer:
            return self.sign(tmsg, signer)
        return tmsg

    def handleOneNodeMsg(self, wrappedMsg):
        raise NotImplementedError("{} must implement this method".format(self))

    async def serviceLifecycle(self) -> None:
        """
        Async function that does the following activities if the node is going:
        (See `Status.going`)

        - check connections (See `checkConns`)
        - maintain connections (See `maintainConnections`)
        """
        if self.isGoing():
            self.checkConns()
            self.maintainConnections()

    def maintainConnections(self, force=False):
        """
        Ensure appropriate connections.

        """
        cur = time.perf_counter()
        if cur > self.nextCheck or force:

            self.nextCheck = cur + (6 if self.isKeySharing else 15)
            # check again in 15 seconds,
            # unless sooner because of retries below

            conns, disconns = self.remotesByConnected()

            for disconn in disconns:
                self.handleDisconnecteRemote(cur, disconn)

            # remove items that have been connected
            for connected in conns:
                self.lastcheck.pop(connected.uid, None)

            self.connectToMissing(cur)

            logger.debug("{} next check for retries in {:.2f} seconds".
                         format(self, self.nextCheck - cur))
            return True
        return False

    def connectToMissing(self, currentTime):
        missing = self.reconcileNodeReg()
        if missing:
            logger.debug("{} found the following missing connections: {}".
                         format(self, ", ".join(missing)))
            if self.connectNicelyUntil is None:
                self.connectNicelyUntil = currentTime + self.reconnectToMissingIn
            if currentTime <= self.connectNicelyUntil:
                names = list(self.nodeReg.keys())
                names.append(self.name)
                nices = set(distributedConnectionMap(names)[self.name])
                for name in nices:
                    logger.debug("{} being nice and waiting for {} to join".
                                 format(self, name))
                missing = missing.difference(nices)

            for name in missing:
                self.connect(name)

    def handleDisconnecteRemote(self, cur, disconn):
        """

        :param disconn: disconnected remote
        """

        # if disconn.main:
        #     logger.trace("{} remote {} is main, so skipping".
        #                  format(self, disconn.uid))
        #     return

        if disconn.joinInProcess():
            logger.trace("{} join already in process, so "
                         "waiting to check for reconnects".
                         format(self))
            self.nextCheck = min(self.nextCheck, cur + self.reconnectToDisconnectedIn)
            return

        if disconn.allowInProcess():
            logger.trace("{} allow already in process, so "
                         "waiting to check for reconnects".
                         format(self))
            self.nextCheck = min(self.nextCheck, cur + self.reconnectToDisconnectedIn)
            return

        if disconn.name not in self.nodeReg:
            # TODO this is almost identical to line 615; make sure we refactor
            regName = self.findInNodeRegByHA(disconn.ha)
            if regName:
                logger.debug("{} forgiving name mismatch for {} with same "
                             "ha {} using another name {}".
                             format(self, regName, disconn.ha, disconn.name))
            else:
                logger.debug("{} skipping reconnect on {} because "
                             "it's not found in the registry".
                             format(self, disconn.name))
                return
        count, last = self.lastcheck.get(disconn.uid, (0, 0))
        # TODO come back to ratcheting retries
        # secsSinceLastCheck = cur - last
        # secsToWait = self.ratchet.get(count)
        # secsToWaitNext = self.ratchet.get(count + 1)
        # if secsSinceLastCheck > secsToWait:
        dname = self.getRemoteName(disconn)
        # extra = "" if not last else "; needed to wait at least {} and " \
        #                             "waited {} (next try will be {} " \
        #                             "seconds)".format(round(secsToWait, 2),
        #                                         round(secsSinceLastCheck, 2),
        #                                         round(secsToWaitNext, 2)))

        logger.debug("{} retrying to connect with {}".
                     format(self.name, dname))
        self.lastcheck[disconn.uid] = count + 1, cur
        # self.nextCheck = min(self.nextCheck,
        #                      cur + secsToWaitNext)
        if disconn.joinInProcess():
            logger.debug("waiting, because join is already in "
                         "progress")
        elif disconn.joined:
            self.nodestack.updateStamp()
            self.nodestack.allow(uid=disconn.uid, cascade=True, timeout=20)
            logger.debug("{} disconnected node is joined".format(
                self), extra={"cli": "STATUS"})
        else:
            self.connect(dname, disconn.uid)

    def findInNodeRegByHA(self, remoteHa):
        regName = [nm for nm, ha in self.nodeReg.items()
                   if self.sameAddr(ha, remoteHa)]
        if len(regName) > 1:
            raise RuntimeError("more than one node registry entry with the "
                               "same ha {}: {}".format(remoteHa, regName))
        if regName:
            return regName[0]
        return None

    def findInRemotesByHA(self, remoteHa):
        remotes = [r for r in self.nodestack.remotes.values()
                   if r.ha == remoteHa]
        assert len(remotes) <= 1
        if remotes:
            return remotes[0]
        return None

    def reconcileNodeReg(self):
        """
        Handle remotes missing from the node registry and clean up old remotes
        no longer in this node's registry.

        1. nice bootstrap
        2. force bootstrap
        3. retry connections

        1. not in remotes
        2.     in remotes, not joined, not allowed, not join in process
        3.     in remotes, not joined, not allowed,     join in process
        4.     in remotes,     joined, not allowed, not allow in process
        5.     in remotes,     joined, not allowed,     allow in process
        6.     in remotes,     joined,     allowed,

        :return: the missing remotes
        """
        matches = set()  # good matches found in nodestack remotes
        legacy = set()  # old remotes that are no longer in registry
        conflicts = set()  # matches found, but the ha conflicts
        logging.debug("{} nodereg is {}".
                      format(self, self.nodeReg.items()))
        logging.debug("{} nodestack is {}".
                      format(self, self.nodestack.remotes.values()))
        for r in self.nodestack.remotes.values():
            if r.name in self.nodeReg:
                if self.sameAddr(r.ha, self.nodeReg[r.name]):
                    matches.add(r.name)
                    logging.debug("{} matched remote is {} {}".
                                  format(self, r.uid, r.ha))
                else:
                    conflicts.add((r.name, r.ha))
                    error("{} ha for {} doesn't match".format(self, r.name))
            else:
                regName = self.findInNodeRegByHA(r.ha)

                # This change fixes test
                # `testNodeConnectionAfterKeysharingRestarted` in
                # `test_node_connection`
                # regName = [nm for nm, ha in self.nodeReg.items() if ha ==
                #            r.ha and (r.joined or r.joinInProcess())]
                logging.debug("{} unmatched remote is {} {}".
                              format(self, r.uid, r.ha))
                if regName:
                    logger.debug("{} forgiving name mismatch for {} with same "
                                 "ha {} using another name {}".
                                 format(self, regName, r.ha, r.name))
                    matches.add(regName)
                else:
                    logger.debug("{} found a legacy remote {} "
                                 "without a matching ha {}".
                                 format(self, r.name, r.ha))
                    legacy.add(r)

        # missing from remotes... need to connect
        missing = set(self.nodeReg.keys()) - matches

        if len(missing) + len(matches) + len(conflicts) != len(self.nodeReg):
            logger.error("Error reconciling nodeReg with remotes")
            logger.error("missing: {}".format(missing))
            logger.error("matches: {}".format(matches))
            logger.error("conflicts: {}".format(conflicts))
            logger.error("nodeReg: {}".format(self.nodeReg.keys()))
            error("Error reconciling nodeReg with remotes; see logs")

        if conflicts:
            error("found conflicting address information {} in registry".format(conflicts))
        if legacy:
            for l in legacy:
                logger.error("{} found legacy entry [{}, {}] in remotes, "
                             "that were not in registry".
                             format(self, l.name, l.ha))
                # TODO probably need to reap
                l.reap()
        return missing

    @staticmethod
    def stackType():
        """
        Return the type of the stack
        """
        return Stack

    def remotesByConnected(self):
        conns, disconns = [], []
        for r in self.nodestack.remotes.values():
            array = conns if Stack.isRemoteConnected(r) else disconns
            array.append(r)
        return conns, disconns

    def getRemoteName(self, remote):
        if remote.name not in self.nodeReg:
            find = [name for name, ha in self.nodeReg.items()
                    if ha == remote.ha]
            assert len(find) == 1
            return find[0]
        return remote.name

    def sameAddr(self, ha, ha2):
        if ha == ha2:
            return True
        elif ha[1] != ha2[1]:
            return False
        else:
            return ha[0] in self.localips and ha2[0] in self.localips
