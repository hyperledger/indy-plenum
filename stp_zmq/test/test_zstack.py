import pytest

from common.exceptions import PlenumTypeError, PlenumValueError
from stp_core.crypto.util import randomSeed
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, prepStacks, chkPrinted
from stp_zmq.test.helper import genKeys, create_and_prep_stacks, \
    check_stacks_communicating, get_file_permission_mask, get_zstack_key_paths
from stp_zmq.zstack import ZStack
from stp_core.common.util import adict
from stp_zmq.test.conftest import BIG_NUM_OF_MSGS


@pytest.fixture
def dummyZStack(tdir, tconf):
    name = 'Alpha'
    alphaP = Printer(name)
    return ZStack(name, ha=genHa(), basedirpath=tdir,
                  msgHandler=alphaP.print, seed=randomSeed(),
                  config=tconf)


def testReconnectRemoteApi(dummyZStack):
    with pytest.raises(PlenumTypeError):
        dummyZStack.reconnectRemote(None)


def testReconnectRemoteWithNameApi(dummyZStack):
    with pytest.raises(PlenumValueError):
        dummyZStack.reconnectRemoteWithName('123')


def testDisconnectByName(dummyZStack):
    with pytest.raises(PlenumValueError):
        dummyZStack.disconnectByName('')


def testAddRemote(dummyZStack):
    with pytest.raises(PlenumValueError):
        dummyZStack.addRemote(None, genHa(), 'verkey', 'pubkey')

    with pytest.raises(PlenumValueError):
        dummyZStack.addRemote('', genHa(), 'verkey', 'pubkey')


def testRestricted2ZStackCommunication(tdir, looper, tconf):
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication only when keys are shared
    :return:
    """
    names = ['Alpha', 'Beta']
    (alpha, beta), (alphaP, betaP) = create_and_prep_stacks(names, tdir,
                                                            looper, tconf)
    check_stacks_communicating(looper, (alpha, beta), (alphaP, betaP))


def testUnrestricted2ZStackCommunication(tdir, looper, tconf):
    """
    Create 2 ZStack and make them send and receive messages.
    Both stacks allow communication even when keys are not shared
    :return:
    """
    names = ['Alpha', 'Beta']
    alphaP = Printer(names[0])
    betaP = Printer(names[1])
    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=alphaP.print,
                   restricted=False, seed=randomSeed(), config=tconf)
    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=betaP.print,
                  restricted=False, seed=randomSeed(), config=tconf)

    prepStacks(looper, alpha, beta, connect=True, useKeys=True)
    alpha.send({'greetings': 'hi'}, beta.name)
    beta.send({'greetings': 'hello'}, alpha.name)

    looper.run(eventually(chkPrinted, alphaP, {'greetings': 'hello'}))
    looper.run(eventually(chkPrinted, betaP, {'greetings': 'hi'}))


def testZStackSendMethodReturnsFalseIfDestinationIsUnknown(tdir, looper, tconf):
    """Case: stp fails on send if a remote close the socket

    Checks:
    1. Connect two stacks
    2. Disconnect a remote from one side
    3. Send a message from disconnected remote
    Expected result: the stack's method 'send' should not
        fail just return False
    """
    names = ['Alpha', 'Beta']
    (alpha, beta), _ = create_and_prep_stacks(names, tdir, looper, tconf)
    # disconnect remote
    alpha.getRemote(beta.name).disconnect()
    # check send message returns False
    assert alpha.send({'greetings': 'hello'}, beta.name)[0] is False


def test_zstack_non_utf8(tdir, looper, tconf):
    """
    ZStack gets a non utf-8 message and does not hand it over to the
    processing method
    :return:
    """
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    (alpha, beta), (alphaP, betaP) = create_and_prep_stacks(names, tdir,
                                                            looper, tconf)

    # Send a utf-8 message and see its received
    for uid in alpha.remotes:
        alpha.transmit(b'{"k1": "v1"}', uid, serialized=True)
    looper.run(eventually(chkPrinted, betaP, {"k1": "v1"}))

    # Send a non utf-8 message and see its not received (by the receiver method)
    for uid in alpha.remotes:
        alpha.transmit(b'{"k2": "v2\x9c"}', uid, serialized=True)
    with pytest.raises(AssertionError):
        looper.run(eventually(chkPrinted, betaP, {"k2": "v2\x9c"}))
    # TODO: A better test where the output of the parsing method is checked
        # requires spyable methods

    # Again send a utf-8 message and see its received (checks if stack is
    # functional after receiving a bad message)
    for uid in alpha.remotes:
        alpha.transmit(b'{"k3": "v3"}', uid, serialized=True)
    looper.run(eventually(chkPrinted, betaP, {"k3": "v3"}))


def test_zstack_creates_keys_with_secure_permissions(tdir):
    any_seed = b'0' * 32
    stack_name = 'aStack'
    key_paths = get_zstack_key_paths(stack_name, tdir)

    ZStack.initLocalKeys(stack_name, tdir, any_seed)

    for file_path in key_paths['secret']:
        assert get_file_permission_mask(file_path) == '600'

    for file_path in key_paths['public']:
        assert get_file_permission_mask(file_path) == '644'


"""
TODO:
* Create ZKitStack, which should maintain a registry and method to check for any
disconnections and do reconnections if found.
* Need a way to run current tests against both stack types, or at least a way to
set a fixture parameter to do so.
* ZNodeStack
* ZClientStack
* test_node_connection needs to work with ZMQ
* test/pool_transactions package

"""


def test_high_load(set_info_log_level, tdir, looper, tconf):
    """
    Checks whether ZStack can cope with high message rate
    """

    letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G',
               'H', 'I', 'J', 'K', 'L', 'M', 'N',
               'O', 'P', 'Q', 'R', 'S', 'T', 'U',
               'V', 'W', 'X', 'Y', 'Z']

    num_of_senders = 3
    num_of_requests_per_sender = BIG_NUM_OF_MSGS

    expected_messages = []
    received_messages = []

    def handler(wrapped_message):
        msg, sender = wrapped_message
        received_messages.append(msg)

    def create_stack(name, handler=None):
        return ZStack(name, ha=genHa(), basedirpath=tdir,
                      msgHandler=handler, restricted=False,
                      seed=randomSeed(), config=tconf)

    senders = [create_stack(letter) for letter in letters[:num_of_senders]]
    gamma = create_stack("Gamma", handler)
    prepStacks(looper, *senders, gamma, connect=True, useKeys=True)

    for sender in senders:
        for i in range(num_of_requests_per_sender):
            msg = {sender.name: i}
            expected_messages.append(msg)
            sender.send(msg, gamma.name)
        looper.runFor(5)

    assert len(received_messages) != 0
    assert len(expected_messages) == len(received_messages), \
        "{} != {}, LAST IS {}"\
        .format(len(expected_messages),
                len(received_messages),
                received_messages[-1])


def testZStackSendRecvHugeDataUnderLimit(set_info_log_level, tdir, looper, tconf):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    # we use json serializer now, so final form will be {'k':'vvv...vvv'}
    # so here we try to prepare exactly tconf.MSG_LEN_LIMIT bytes after serialization
    msg = {'k': 'v' * (tconf.MSG_LEN_LIMIT - len("{'k':''}"))}

    betaHandler = [False]

    def recvHandlerAlpha(wrpMsg):
        pass

    def recvHandlerBeta(wrpMsg):
        rmsg, frm = wrpMsg
        betaHandler[0] = True
        assert frm == 'Alpha'
        assert rmsg == msg

    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=recvHandlerAlpha, restricted=True,
                   config=adict(**tconf.__dict__), msgRejectHandler=None)

    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=recvHandlerBeta, restricted=True,
                  config=adict(**tconf.__dict__), msgRejectHandler=None)

    assert len(alpha.serializeMsg(msg)) == tconf.MSG_LEN_LIMIT

    prepStacks(looper, *(alpha, beta), connect=True, useKeys=True)

    stat = alpha.send(msg, beta.name)
    assert stat[0] is True

    looper.runFor(5)

    assert betaHandler[0] is True


def testZStackSendHugeDataOverLimit(set_info_log_level, tdir, looper, tconf):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    # we use json serializer now, so final form will be {'k':'vvv...vvv'}
    # so here we try to prepare exactly tconf.MSG_LEN_LIMIT + 1 bytes after serialization
    msg = {'k': 'v' * (tconf.MSG_LEN_LIMIT - len("{'k':''}") + 1)}

    betaHandlers = [False, False]

    def recvHandlerAlpha(wrpMsg):
        pass

    def recvHandlerBeta(wrpMsg):
        rmsg, frm = wrpMsg
        betaHandlers[0] = True
        assert frm is not None
        assert rmsg is not None

    def rejectHandlerBeta(reason, frm):
        betaHandlers[1] = True
        assert reason is not None
        assert frm is not None

    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=recvHandlerAlpha, restricted=True,
                   config=adict(**tconf.__dict__), msgRejectHandler=None)

    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=recvHandlerBeta, restricted=True,
                  config=adict(**tconf.__dict__), msgRejectHandler=rejectHandlerBeta)

    assert len(alpha.serializeMsg(msg)) == (tconf.MSG_LEN_LIMIT + 1)

    prepStacks(looper, *(alpha, beta), connect=True, useKeys=True)

    stat = alpha.send(msg, beta.name)
    assert stat[0] is False
    assert 'exceeded allowed limit of {}'.format(
        tconf.MSG_LEN_LIMIT) in stat[1]

    looper.runFor(5)

    assert betaHandlers[0] is False
    assert betaHandlers[1] is False


def testZStackRecvHugeDataOverLimit(set_info_log_level, tdir, looper, tconf):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    # we use json serializer now, so final form will be {'k':'vvv...vvv'}
    # so here we try to prepare exactly tconf.MSG_LEN_LIMIT + 1 bytes after serialization
    msg = {'k': 'v' * (tconf.MSG_LEN_LIMIT - len("{'k':''}") + 1)}

    betaHandlers = [False, False]

    def recvHandlerAlpha(wrpMsg):
        pass

    def recvHandlerBeta(wrpMsg):
        rmsg, frm = wrpMsg
        betaHandlers[0] = True
        assert frm is not None
        assert rmsg is not None

    def rejectHandlerBeta(reason, frm):
        betaHandlers[1] = True
        assert 'exceeded allowed limit of {}'.format(
            tconf.MSG_LEN_LIMIT) in reason
        assert frm == 'Alpha'

    alpha = ZStack(names[0], ha=genHa(), basedirpath=tdir, msgHandler=recvHandlerAlpha, restricted=True,
                   config=adict(**tconf.__dict__), msgRejectHandler=None)

    beta = ZStack(names[1], ha=genHa(), basedirpath=tdir, msgHandler=recvHandlerBeta, restricted=True,
                  config=adict(**tconf.__dict__), msgRejectHandler=rejectHandlerBeta)

    bytemsg = alpha.serializeMsg(msg)
    assert len(bytemsg) == (tconf.MSG_LEN_LIMIT + 1)

    prepStacks(looper, *(alpha, beta), connect=True, useKeys=True)

    stat = alpha._remotes['Beta'].socket.send(bytemsg, copy=False, track=True)
    assert stat

    looper.runFor(5)

    assert betaHandlers[0] is False
    assert betaHandlers[1] is True


def test_queue_size_limit_set(tdir, tconf):
    stack = ZStack("Alpha", ha=genHa(), basedirpath=tdir, msgHandler=None,
                   restricted=False, seed=randomSeed(), config=tconf)
    stack.start()
    assert stack.listener.get_hwm() == 0
    stack.stop()

    queue_size = 100
    stack = ZStack("Alpha", ha=genHa(), basedirpath=tdir, msgHandler=None,
                   restricted=False, seed=randomSeed(), config=tconf, queue_size=queue_size)
    stack.start()
    assert stack.listener.get_hwm() == queue_size
    stack.stop()
