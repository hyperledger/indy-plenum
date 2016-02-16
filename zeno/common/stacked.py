import logging
import sys
import time
from collections import Callable
from collections import deque
from typing import Any, Set
from typing import Dict, NamedTuple
from typing import Mapping
from typing import Tuple

from raet.road.estating import RemoteEstate
from raet.road.stacking import RoadStack
from raet.road.transacting import Joiner, Allower

from zeno.common.exceptions import RemoteNotFound
from zeno.common.motor import Motor
from zeno.common.ratchet import Ratchet
from zeno.common.request_types import Request, Batch, TaggedTupleBase
from zeno.common.util import error, distributedConnectionMap, \
    MessageProcessor, getlogger, checkPortAvailable

logger = getlogger()

HA = NamedTuple("HA", [
    ("host", str),
    ("port", int)])

# this overrides the defaults
Joiner.RedoTimeoutMin = 1.0
Joiner.RedoTimeoutMax = 2.0
Joiner.Timeout = 30

Allower.RedoTimeoutMin = 1.0
Allower.RedoTimeoutMax = 2.0
Allower.Timeout = 30


class Stack(RoadStack):
    def __init__(self, *args, **kwargs):
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
                logger.error("Error servicing stack: {}".format(ex))
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
        if not checkPortAvailable(stack['ha']):
            error("Address {} already in use".format(stack['ha']))
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

    def _enqueue(self, msg: Any, rid: int) -> None:
        """
        Enqueue the message into the remote's queue.

        :param msg: the message to enqueue
        :param rid: the id of the remote node
        """
        payload = self.prepForSending(msg)
        if rid not in self.outBoxes:
            self.outBoxes[rid] = deque()
        self.outBoxes[rid].append(payload)

    def _enqueueIntoAllRemotes(self, msg: Any) -> None:
        """
        Enqueue the specified message into all the remotes in the nodestack.

        :param msg: the message to enqueue
        """
        for rid in self.nodestack.remotes.keys():
            self._enqueue(msg, rid)

    def send(self, msg: Any, *rids: int) -> None:
        """
        Enqueue the given message into the outBoxes of the specified remotes
         or into the outBoxes of all the remotes if rids is None

        :param msg: the message to enqueue
        :param rids: ids of the remotes to whose outBoxes
         this message must be enqueued
        """
        if rids:
            for r in rids:
                self._enqueue(msg, r)
        else:
            self._enqueueIntoAllRemotes(msg)

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

    def __init__(self, stackParams: dict, nodeReg: Dict[str, HA]):
        super().__init__()
        self.name = stackParams["name"]
        self.bootstrapped = False

        self.nodeStackParams = stackParams
        self.nodestack = None  # type: Stack

        self.lastcheck = {}  # type: Dict[int, Tuple[int, float]]
        self.ratchet = Ratchet(a=8, b=0.198, c=-4, base=8, peak=3600)
        self.nodeReg = nodeReg

        # holds the last time we checked remotes
        self.nextCheck = 0

        self._conns = set()  # type: Set[str]

    def __repr__(self):
        return self.name

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

    def connect(self, name) -> int:
        """
        Connect to the node specified by name.

        :param name: name of the node to connect to
        :type name: str or (HA, tuple)
        :return: the uid of the remote estate
        """
        if isinstance(name, (HA, tuple)):
            other_node_ha = name
        elif isinstance(name, str):
            other_node_ha = self.nodeReg[name]
        else:
            raise AttributeError()
        remote = RemoteEstate(stack=self.nodestack,
                              ha=other_node_ha)
        self.nodestack.addRemote(remote)
        # updates the store time so the join timer is accurate
        self.nodestack.updateStamp()
        self.nodestack.join(uid=remote.uid, cascade=True, timeout=60)
        logger.info("{} looking for {} at {}:{}".
                    format(self.name, name, *other_node_ha),
                    extra={"cli": "PLAIN"})
        return remote.uid

    def sign(self, msg: Mapping) -> Mapping:
        """
        No signing is implemented in NodeStacked. Returns the msg as it is.

        :param msg: the message to sign
        """
        return msg  # don't sign by default

    def prepForSending(self, msg: Mapping) -> Mapping:
        """
        Return a dictionary form of the message

        :param msg: the message to be sent
        :raises: ValueError if msg cannot be converted to an appropriate format for transmission
        """
        if isinstance(msg, TaggedTupleBase):
            tmsg = msg.melted()
        elif isinstance(msg, Request):
            tmsg = msg.__getstate__()
        else:
            raise ValueError("Message cannot be converted to an appropriate format for transmission")
        smsg = self.sign(tmsg)
        return smsg

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


    def maintainConnections(self):
        """
        Try to connect to all the nodes.
        """
        self._retryConnections()

    def _retryConnections(self):
        """
        Try connecting to disconnected nodes again.

        :return: whether the retry attempt was successful
        """
        cur = time.perf_counter()
        if cur > self.nextCheck:
            if any(r.joinInProcess() or r.allowInProcess()
                   for r in self.nodestack.remotes.values()):
                logger.trace("{} joins or allows already in process, so "
                             "waiting to check for reconnects".format(self))
                self.nextCheck = cur + 3
                return False
            self.nextCheck = cur + 15
            # check again in 15 seconds,
            # unless sooner because of retries below

            conns, disconns = self.remotesByConnected()

            for disconn in disconns:
                if disconn.name not in self.nodeReg:
                    logger.debug("{} skipping reconnect on {} because "
                                 "it's not found in the registry".
                                 format(self, disconn.name))
                    continue
                count, last = self.lastcheck.get(disconn.uid, (0, 0))
                secsSinceLastCheck = cur - last
                secsToWait = self.ratchet.get(count)
                secsToWaitNext = self.ratchet.get(count + 1)
                if secsSinceLastCheck > secsToWait:
                    dname = self.getRemoteName(disconn)
                    logger.debug("{} retrying to connect with {}".
                                 format(self.name, dname) +
                                 ("" if not last else "; needed to wait at "
                                                      "least {} and waited "
                                                      "{} (next try will "
                                                      "be {} seconds)".
                                  format(round(secsToWait, 2),
                                         round(secsSinceLastCheck, 2),
                                         round(secsToWaitNext, 2))))
                    self.lastcheck[disconn.uid] = count + 1, cur
                    self.nextCheck = min(self.nextCheck,
                                         cur + secsToWaitNext)
                    if disconn.joinInProcess():
                        logger.debug("waiting, because join is already in "
                                     "progress")
                    else:
                        logger.info("{} reconnecting to {} at {}:{}".
                                    format(self, dname, *disconn.ha))
                        # update the store time so the allow timer works
                        self.nodestack.updateStamp()
                        self.nodestack.allow(uid=disconn.uid,
                                             cascade=True)
            # remove items that have been connected
            for connected in conns:
                self.lastcheck.pop(connected.uid, None)

            logger.debug("{} next check for retries in {:.2f} seconds".
                         format(self, self.nextCheck - cur))
            return True
        return False

    def bootstrap(self, forced: bool=None):
        """
        Connect to all nodes in the node registry.
        """
        logging.info("{} is bootstrapping, forced is {}".format(self, forced),
                     extra={"cli": False})
        missing = self.reconcileNodeReg()
        if missing:
            logger.debug("{} found the following missing connections: {}".format(self, ", ".join(missing)))
            if not forced:
                names = list(self.nodeReg.keys())
                names.append(self.name)
                nices = set(distributedConnectionMap(names)[self.name])
                for name in nices:
                    logger.debug("{} being nice and waiting for {} to join".format(self, name))
                missing = missing.difference(nices)
            for name in missing:
                self.connect(name)
        self.bootstrapped = True

    def reconcileNodeReg(self):
        """
        Handle remotes missing from the node registry and clean up old remotes no longer in this node's registry.

        :return: the missing remotes
        """
        matches = set()  # good matches found in nodestack remotes
        legacy = set()  # old remotes that are no longer in registry
        conflicts = set()  # matches found, but the ha conflicts
        logging.debug("{}'s nodereg is {}"
                      .format(self, self.nodeReg.items()))
        logging.debug("{}'s nodestack is {}"
            .format(self, self.nodestack.remotes.values()))
        for r in self.nodestack.remotes.values():
            if r.name in self.nodeReg:
                if r.ha == self.nodeReg[r.name]:
                    matches.add(r.name)
                    logging.debug("matched remote is {} {}".format(r.uid, r.ha))
                else:
                    conflicts.add((r.name, r.ha))
                    error("ha for {} doesn't match".format(r.name))
            else:
                regName = [nm for nm, ha in self.nodeReg.items() if ha == r.ha]
                logging.debug("unmatched remote is {} {}".format(r.uid, r.ha))
                # assert len(regName) == 1
                if regName:
                    logger.debug("forgiving name mismatch for {} with same "
                                 "ha {} using another name {}".
                                 format(regName, r.ha, r.name))
                    matches.add(regName[0])
                else:
                    logger.debug("{} found a legacy remote {} "
                                 "without a matching ha {}".
                                 format(self, r.name, r.ha))
                    legacy.add(r)

        # missing from remotes... need to connect
        missing = set(nm for nm, ha in self.nodeReg.items() if nm not in matches)

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

                # TODO Remove this. Why are we raising error. Someone might
                # attempt to connect to a node and might end up in its nodestack

                # error("found legacy entries {} in remotes, that were not "
                #       "in registry".format(legacy))
                # this could happen if we are somehow re-using the same temp directory
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
