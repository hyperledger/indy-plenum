"""
A client in an RBFT system.
Client sends requests to each of the nodes,
and receives result of the request execution from nodes.
"""
import base64
import json
import logging
import os
import time
from collections import deque, OrderedDict
from typing import List, Union, Dict, Optional, Mapping, Tuple, Set

from raet.nacling import Privateer
from raet.raeting import AutoMode
from ledger.merkle_verifier import MerkleVerifier
from ledger.serializers.json_serializer import JsonSerializer
from ledger.util import F, STH

from plenum.client.signer import Signer, SimpleSigner
from plenum.client.wallet import Wallet
from plenum.common.motor import Motor
from plenum.common.stacked import NodeStack
from plenum.common.startable import Status
from plenum.common.txn import REPLY, CLINODEREG, TXN_TYPE, TARGET_NYM, PUBKEY, \
    DATA, ALIAS, NEW_STEWARD, NEW_NODE, NODE_IP, NODE_PORT, CLIENT_IP, \
    CLIENT_PORT, CHANGE_HA, CHANGE_KEYS, VERKEY, NEW_CLIENT
from plenum.common.types import Request, Reply, OP_FIELD_NAME, f, HA
from plenum.common.util import getMaxFailures, getlogger
from plenum.persistence.wallet_storage_file import WalletStorageFile

logger = getlogger()


class Client(Motor):

    def __init__(self,
                 name: str,
                 nodeReg: Dict[str, HA]=None,
                 ha: Union[HA, Tuple[str, int]]=None,
                 lastReqId: int = 0,
                 signer: Signer=None,
                 signers: Dict[str, Signer]=None,
                 basedirpath: str=None,
                 wallet: Wallet=None):
        """
        Creates a new client.

        :param name: unique identifier for the client
        :param nodeReg: names and host addresses of all nodes in the pool
        :param ha: tuple of host and port
        :param lastReqId: Request Id of the last request sent by client
        :param signer: Signer; mutually exclusive of signers
        :param signers: Dict of identifier -> Signer; useful for clients that
            need to support multiple signers
        """
        self.name = name
        self.lastReqId = lastReqId
        self.minimumNodes = getMaxFailures(len(nodeReg)) + 1

        cliNodeReg = OrderedDict()
        for nm in nodeReg:
            val = nodeReg[nm]
            if len(val) == 3:
                ((ip, port), verkey, pubkey) = val
            else:
                ip, port = val
            cliNodeReg[nm] = HA(ip, port)

        self.nodeReg = cliNodeReg

        cha = ha if isinstance(ha, HA) else HA(*ha)
        stackargs = dict(name=name,
                         ha=cha,
                         main=False,  # stops incoming vacuous joins
                         auto=AutoMode.always)
        if basedirpath:
            stackargs['basedirpath'] = basedirpath
        self.created = time.perf_counter()

        # noinspection PyCallingNonCallable
        self.nodestack = self.nodeStackClass(stackargs,
                                   self.handleOneNodeMsg,
                                   self.nodeReg)
        self.nodestack.onConnsChanged = self.onConnsChanged
        self.nodestack.sign = self.sign

        logger.info("Client initialized with the following node registry:")
        lengths = [max(x) for x in zip(*[
            (len(name), len(host), len(str(port)))
            for name, (host, port) in nodeReg.items()])]
        fmt = "    {{:<{}}} listens at {{:<{}}} on port {{:>{}}}".format(
            *lengths)
        for name, (host, port) in nodeReg.items():
            logger.info(fmt.format(name, host, port))

        Motor.__init__(self)

        self.inBox = deque()

        if signer and signers:
            raise ValueError("only one of 'signer' or 'signers' can be used")

        clientDataDir = os.path.join(basedirpath, "data", "clients", self.name)
        clientDataDir = os.path.expanduser(clientDataDir)
        self.wallet = wallet or Wallet(WalletStorageFile(clientDataDir))
        signers = None     # type: Dict[str, Signer]
        self.defaultIdentifier = None
        if not self.wallet.signers:
            if signer:
                signers = {signer.identifier: signer}
                self.defaultIdentifier = signer.identifier
            elif signers:
                signers = signers
            else:
                signers = self.setupDefaultSigner()

            for s in signers.values():
                self.wallet.addSigner(signer=s)
        else:
            if len(self.wallet.signers) == 1:
                self.defaultIdentifier = list(self.wallet.signers.values())[0].identifier

        self.nodestack.connectNicelyUntil = 0  # don't need to connect
        # nicely as a client

        # Temporary node registry. Used to keep locations of unknown validators
        # until location confirmed by consensus. Key is the name of client stack
        # of unknown validator and value is a dictionary with location, i.e HA
        # as key and value as set of names of known validators vetting that
        # location
        self.tempNodeReg = {}  # type: Dict[str, Dict[tuple, Set[str]]]

    @property
    def nodeStackClass(self) -> NodeStack:
        return NodeStack

    @property
    def signers(self):
        return self.wallet.signers

    def setupDefaultSigner(self):
        """
        Create one SimpleSigner and add it to signers
        against the client's name.
        """
        signers = {self.name: SimpleSigner(self.name)}
        self.defaultIdentifier = self.name
        return signers

    def start(self, loop):
        oldstatus = self.status
        super().start(loop)
        if oldstatus in Status.going():
            logger.info("{} is already {}, so start has no effect".
                        format(self, self.status.name))
        else:
            self.nodestack.start()
            self.nodestack.maintainConnections()

    async def prod(self, limit) -> int:
        """
        async function that returns the number of events

        :param limit: The number of messages to be processed
        :return: The number of events up to a prescribed `limit`
        """
        s = await self.nodestack.service(limit)
        if self.isGoing():
            await self.nodestack.serviceLifecycle()
        self.nodestack.flushOutBoxes()
        return s

    def createRequest(self, operation: Mapping, identifier: str=None) -> Request:
        """
        Client creates request which include requested operation and request Id

        :param operation: requested operation
        :return: New client request
        """

        request = Request(identifier or self.defaultIdentifier,
                          self.lastReqId + 1,
                          operation)
        self.lastReqId += 1
        return request

    def submit(self, *operations: Mapping, identifier: str=None) -> List[Request]:
        """
        Sends an operation to the consensus pool

        :param operations: a sequence of operations
        :param identifier: an optional identifier to use for signing
        :return: A list of client requests to be sent to the nodes in the system
        """
        identifier = identifier if identifier else self.defaultIdentifier
        requests = []
        for op in operations:
            request = self.createRequest(op, identifier)
            self.nodestack.send(request, signer=self.getSigner(identifier))
            requests.append(request)
        return requests

    def getSigner(self, identifier: str=None):
        """
        Look up and return a signer corresponding to the identifier specified.
        Return None if not found.
        """
        try:
            return self.signers[identifier or self.defaultIdentifier]
        except KeyError:
            return None

    def sign(self, msg: Dict, signer: Signer) -> Dict:
        """
        Signs the message if a signer is configured

        :param msg: Message to be signed
        :return: message
        """
        if f.SIG.nm not in msg or not msg[f.SIG.nm]:
            if signer:
                msg[f.SIG.nm] = signer.sign(msg)
            else:
                logger.warning("{} signer not configured so not signing {}".
                               format(self, msg))
        return msg

    def handleOneNodeMsg(self, wrappedMsg) -> None:
        """
        Handles single message from a node, and appends it to a queue

        :param wrappedMsg: Reply received by the client from the node
        """
        self.inBox.append(wrappedMsg)
        msg, frm = wrappedMsg
        logger.debug("Client {} got msg from node {}: {}".
                     format(self.name, frm, msg),
                     extra={"cli": msg.get(OP_FIELD_NAME) != CLINODEREG})
        if OP_FIELD_NAME in msg and msg[OP_FIELD_NAME] == CLINODEREG:
            cliNodeReg = msg[f.NODES.nm]
            for name, ha in cliNodeReg.items():
                self.newValidatorDiscovered(name, tuple(ha), frm)

    def newValidatorDiscovered(self, stackName: str, stackHA: Tuple[str, int],
                               frm: str):
        if stackName in self.nodeReg and \
                        stackHA == tuple(self.nodeReg[stackName]):
            # TODO: What if a malicious validator is trying to communicate an
            # incorrect ha to a client? Probably blacklist it.
            logger.debug("{} already present in nodeReg".format(stackName))
            return
        else:
            if stackName not in self.tempNodeReg:
                self.tempNodeReg[stackName] = {}
            if stackHA not in self.tempNodeReg[stackName]:
                self.tempNodeReg[stackName][stackHA] = set()
            self.tempNodeReg[stackName][stackHA].add(frm)
            f = getMaxFailures(len(self.nodeReg))

            if len(self.tempNodeReg[stackName][stackHA]) > f:
                if stackName not in self.nodeReg:
                    self.newNodeFound(stackName, stackHA)
                else:
                    self.nodestack.keep.clearRemoteData(stackName)
                    rid = self.nodestack.removeRemoteByName(stackName)
                    self.nodestack.outBoxes.pop(rid, None)
                    self.nodeReg[stackName] = HA(*stackHA)

    def newNodeFound(self, name: str, ha: Tuple[str, int]):
        self.nodeReg[name] = HA(*ha)
        del self.tempNodeReg[name]

    def _statusChanged(self, old, new):
        # do nothing for now
        pass

    def onStopping(self, *args, **kwargs):
        self.nodestack.nextCheck = 0
        if self.nodestack.opened:
            self.nodestack.close()

    def getReply(self, reqId: int) -> Optional[Reply]:
        """
        Accepts reply message from node if the reply is matching

        :param reqId: Request Id
        :return: Reply message only when valid and matching
        (None, NOT_FOUND)
        (None, UNCONFIRMED) f+1 not reached
        (reply, CONFIRMED) f+1 reached
        """
        try:
            cons = self.hasConsensus(reqId)
        except KeyError:
            return None, "NOT_FOUND"
        if cons:
            return cons, "CONFIRMED"
        return None, "UNCONFIRMED"

    def getRepliesFromAllNodes(self, reqId: int):
        """
        Accepts a request ID and return a list of results from all the nodes
        for that request

        :param reqId: Request ID
        :return: list of request results from all nodes
        """
        return {frm: msg for msg, frm in self.inBox
                if msg[OP_FIELD_NAME] == REPLY and
                msg[f.RESULT.nm][f.REQ_ID.nm] == reqId}

    def hasConsensus(self, reqId: int) -> Optional[str]:
        """
        Accepts a request ID and returns True if consensus was reached
        for the request or else False

        :param reqId: Request ID
        """
        replies = self.getRepliesFromAllNodes(reqId)
        if not replies:
            raise KeyError(reqId)  # NOT_FOUND
        # Check if at least f+1 replies are received or not.
        f = getMaxFailures(len(self.nodeReg))
        if f + 1 > len(replies):
            return False  # UNCONFIRMED
        else:
            onlyResults = {frm: reply['result'] for frm, reply in
                           replies.items()}
            resultsList = list(onlyResults.values())
            # if all the elements in the resultList are equal - consensus
            # is reached.
            if all(result == resultsList[0] for result in resultsList):
                return resultsList[0]  # CONFIRMED
            else:
                logging.error(
                    "Received a different result from at least one of the nodes..")
                # Now we need to know the counts of different results and so.
                jsonResults = [json.dumps(result, sort_keys=True) for result in
                               resultsList]
                # counts dictionary for calculating the count of different
                # results
                counts = {}
                for jresult in jsonResults:
                    counts[jresult] = counts.get(jresult, 0) + 1
                if counts[max(counts, key=counts.get)] > f + 1:
                    # CONFIRMED, as f + 1 matching results found
                    return json.loads(max(counts, key=counts.get))
                else:
                    # UNCONFIRMED, as f + 1 matching results are not found
                    return False

    def showReplyDetails(self, reqId: int):
        """
        Accepts a request ID and prints the reply details

        :param reqId: Request ID
        """
        replies = self.getRepliesFromAllNodes(reqId)
        replyInfo = "Node {} replied with result {}"
        if replies:
            for frm, reply in replies.items():
                print(replyInfo.format(frm, reply['result']))
        else:
            print("No replies received from Nodes!")

    def onConnsChanged(self, newConns: Set[str], lostConns: Set[str]):
        """
        Modify the current status of the client based on the status of the
        connections changed.
        """
        if self.isGoing():
            if len(self.nodestack.conns) == len(self.nodeReg):
                self.status = Status.started
            elif len(self.nodestack.conns) >= self.minimumNodes:
                self.status = Status.started_hungry

    def submitNewClient(self, typ, name: str, pkseed: bytes, sigseed: bytes):
        assert typ in (NEW_STEWARD, NEW_CLIENT), "Invalid type {}".format(typ)
        newSigner = SimpleSigner(seed=sigseed)
        priver = Privateer(pkseed)
        req, = self.submit({
            TXN_TYPE: typ,
            TARGET_NYM: newSigner.verstr,
            DATA: {
                PUBKEY: priver.pubhex.decode(),
                ALIAS: name
            }
        })
        return req

    def submitNewSteward(self, name: str, pkseed: bytes, sigseed: bytes):
        return self.submitNewClient(NEW_STEWARD, name, pkseed, sigseed)

    def submitNewNode(self, name: str, pkseed: bytes, sigseed: bytes,
                      nodeStackHa: HA, clientStackHa: HA):
        (nodeIp, nodePort), (clientIp, clientPort) = nodeStackHa, clientStackHa
        newSigner = SimpleSigner(seed=sigseed)
        priver = Privateer(pkseed)
        req, = self.submit({
            TXN_TYPE: NEW_NODE,
            TARGET_NYM: newSigner.verstr,
            DATA: {
                NODE_IP: nodeIp,
                NODE_PORT: nodePort,
                CLIENT_IP: clientIp,
                CLIENT_PORT: clientPort,
                PUBKEY: priver.pubhex.decode(),
                ALIAS: name
            }
        })
        return req

    # TODO: Shouldn't the nym be fetched from the ledger
    def submitNodeIpChange(self, name: str, nym: str, nodeStackHa: HA,
                           clientStackHa: HA):
        (nodeIp, nodePort), (clientIp, clientPort) = nodeStackHa, clientStackHa
        req, = self.submit({
            TXN_TYPE: CHANGE_HA,
            TARGET_NYM: nym,
            DATA: {
                NODE_IP: nodeIp,
                NODE_PORT: nodePort,
                CLIENT_IP: clientIp,
                CLIENT_PORT: clientPort,
                ALIAS: name
            }
        })
        return req

    def submitNodeKeysChange(self, name: str, nym: str, verkey: str, pubkey: str):
        req, = self.submit({
            TXN_TYPE: CHANGE_KEYS,
            TARGET_NYM: nym,
            DATA: {
                PUBKEY: pubkey,
                VERKEY: verkey,
                ALIAS: name
            }
        })
        return req

    @staticmethod
    def verifyMerkleProof(*replies: Tuple[Reply]) -> bool:
        """
        Verifies the correctness of the merkle proof provided in the reply from
        the node. Returns True if verified to be correct, throws an exception
        otherwise.

        :param replies: One or more replies for which Merkle Proofs have to be
        verified
        :raises ProofError: The proof is invalid
        :return: True
        """
        verifier = MerkleVerifier()
        serializer = JsonSerializer()
        for r in replies:
            seqNo = r[f.RESULT.nm][F.seqNo.name]
            rootHash = base64.b64decode(r[f.RESULT.nm][F.rootHash.name].encode())
            auditPath = [base64.b64decode(
                a.encode()) for a in r[f.RESULT.nm][F.auditPath.name]]
            filtered = ((k, v) for (k, v) in r[f.RESULT.nm].iteritems()
                        if k not in
                        [F.auditPath.name, F.seqNo.name, F.rootHash.name])
            result = serializer.serialize(dict(filtered))
            verifier.verify_leaf_inclusion(result, seqNo-1,
                                           auditPath,
                                           STH(tree_size=seqNo,
                                               sha256_root_hash=rootHash))
        return True


class ClientProvider:
    """
    Lazy client provider that takes a callback that returns a Client.
    It also shadows the client and when the client's any attribute is accessed
    the first time, it creates the client object using the callback.
    """

    def __init__(self, clientGenerator=None):
        """
        :param clientGenerator: Client generator
        """
        self.clientGenerator = clientGenerator
        self.client = None

    def __getattr__(self, attr):
        if attr not in ["clientGenerator", "client"]:
            if not self.client:
                self.client = self.clientGenerator()
            if hasattr(self.client, attr):
                return getattr(self.client, attr)
            raise AttributeError(
                "Client has no attribute named {}".format(attr))
