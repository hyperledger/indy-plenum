from functools import partial

from plenum.client.client import Client, ClientProvider
from plenum.client.wallet import Wallet
from plenum.common.log import getlogger
from plenum.common.port_dispenser import genHa
from plenum.common.stacked import NodeStack
from plenum.common.txn import REQACK, REQNACK, REPLY
from plenum.common.types import Identifier, HA, OP_FIELD_NAME, f
from plenum.common.util import bootstrapClientKeys
from plenum.common.error import error
from plenum.test.test_stack import StackedTester, getTestableStack
from plenum.test.testable import Spyable


logger = getlogger()


@Spyable(methods=[Client.handleOneNodeMsg])
class TestClient(Client, StackedTester):
    @property
    def nodeStackClass(self) -> NodeStack:
        return getTestableStack(NodeStack)

    def handleOneNodeMsg(self, wrappedMsg, excludeFromCli=None) -> None:
        super().handleOneNodeMsg(wrappedMsg, excludeFromCli=excludeFromCli)


def genTestClient(nodes = None,
                  nodeReg=None,
                  tmpdir=None,
                  testClientClass=TestClient,
                  identifier: Identifier=None,
                  verkey: str=None,
                  bootstrapKeys=True,
                  ha=None,
                  usePoolLedger=False,
                  name=None) -> TestClient:
    if not usePoolLedger:
        nReg = nodeReg
        if nodeReg:
            assert isinstance(nodeReg, dict)
        elif hasattr(nodes, "nodeReg"):
            nReg = nodes.nodeReg.extractCliNodeReg()
        else:
            error("need access to nodeReg")
        for k, v in nReg.items():
            assert type(k) == str
            assert (type(v) == HA or type(v[0]) == HA)
    else:
        logger.debug("TestClient using pool ledger")
        nReg = None

    ha = genHa() if not ha else ha

    tc = testClientClass(name,
                         nodeReg=nReg,
                         ha=ha,
                         basedirpath=tmpdir)
    w = None  # type: Wallet
    if bootstrapKeys and nodes:
        if not identifier or not verkey:
            # no identifier or verkey were provided, so creating a wallet
            w = Wallet("test")
            w.addIdentifier()
            identifier = w.defaultId
            verkey = w.getVerkey()
        bootstrapClientKeys(identifier, verkey, nodes)
    return tc, w


def genTestClientProvider(nodes = None,
                          nodeReg=None,
                          tmpdir=None,
                          clientGnr=genTestClient):
    clbk = partial(clientGnr, nodes, nodeReg, tmpdir)
    return ClientProvider(clbk)


def getAcksFromInbox(client, reqId, maxm=None):
    acks = set()
    for msg, sender in client.inBox:
        if msg[OP_FIELD_NAME] == REQACK and msg[f.REQ_ID.nm] == reqId:
            acks.add(sender)
            if maxm and len(acks) == maxm:
                break
    return acks


def getNacksFromInbox(client, reqId, maxm=None):
    nacks = {}
    for msg, sender in client.inBox:
        if msg[OP_FIELD_NAME] == REQNACK and msg[f.REQ_ID.nm] == reqId:
            nacks[sender] = msg[f.REASON.nm]
            if maxm and len(nacks) == maxm:
                break
    return nacks


def getRepliesFromInbox(client, reqId, maxm=None) -> list:
    replies = {}
    for msg, sender in client.inBox:
        if msg[OP_FIELD_NAME] == REPLY and msg[f.RESULT.nm][f.REQ_ID.nm] == reqId:
            replies[sender] = msg
            if maxm and len(replies) == maxm:
                break
    return replies