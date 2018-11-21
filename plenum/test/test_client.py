from plenum.common.stacks import nodeStackClass
from plenum.common.txn_util import get_type
from stp_core.network.network_interface import NetworkInterface

from plenum.client.client import Client
from stp_core.common.log import getlogger
from plenum.common.constants import REQACK, REQNACK, REPLY
from plenum.common.types import f
from plenum.test.test_stack import StackedTester, getTestableStack
from plenum.test.testable import spyable
from plenum.common.constants import OP_FIELD_NAME

logger = getlogger()

client_spyables = [Client.handleOneNodeMsg,
                   Client.resendRequests,
                   Client.send,
                   Client.submitReqs]


@spyable(methods=client_spyables)
class TestClient(Client, StackedTester):
    def __init__(self, *args, **kwargs):
        self.NodeStackClass = nodeStackClass
        super().__init__(*args, **kwargs)

    @property
    def nodeStackClass(self) -> NetworkInterface:
        return getTestableStack(self.NodeStackClass)

    def handleOneNodeMsg(self, wrappedMsg, excludeFromCli=None) -> None:
        super().handleOneNodeMsg(wrappedMsg, excludeFromCli=excludeFromCli)

    def prepare_for_state(self, result):
        if get_type(result) == "buy":
            from plenum.test.test_node import TestDomainRequestHandler
            key, value = TestDomainRequestHandler.prepare_buy_for_state(result)
            return key, value


def getAcksFromInbox(client, reqId, maxm=None) -> set:
    acks = set()
    for msg, sender in client.inBox:
        if msg[OP_FIELD_NAME] == REQACK and msg[f.REQ_ID.nm] == reqId:
            acks.add(sender)
            if maxm and len(acks) == maxm:
                break
    return acks


def getNacksFromInbox(client, reqId, maxm=None) -> dict:
    nacks = {}
    for msg, sender in client.inBox:
        if msg[OP_FIELD_NAME] == REQNACK and msg[f.REQ_ID.nm] == reqId:
            nacks[sender] = msg[f.REASON.nm]
            if maxm and len(nacks) == maxm:
                break
    return nacks


def getRepliesFromInbox(client, reqId, maxm=None) -> dict:
    replies = {}
    for msg, sender in client.inBox:
        if msg[OP_FIELD_NAME] == REPLY and msg[f.RESULT.nm][f.REQ_ID.nm] == reqId:
            replies[sender] = msg
            if maxm and len(replies) == maxm:
                break
    return replies
