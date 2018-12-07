from copy import copy
import pytest

from plenum.common.stacks import nodeStackClass
from plenum.common.util import randomString
from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa

from stp_core.test.helper import Printer, prepStacks, checkStacksConnected
from stp_zmq.kit_zstack import KITZStack
from stp_zmq.test.helper import genKeys
from stp_zmq.zstack import Quota


@pytest.fixture()
def registry():
    return {
        'Alpha': genHa(),
        'Beta': genHa(),
        'Gamma': genHa(),
        'Delta': genHa()
    }


@pytest.fixture()
def connection_timeout(tconf):
    # TODO: the connection may not be established for the first try because
    # some of the stacks may not have had a remote yet (that is they haven't had yet called connect)
    return 2 * tconf.RETRY_TIMEOUT_RESTRICTED + 1


def create_fake_nodestack(tdir, tconf, registry, name='Node1'):
    def msgHandler(msg):
        pass

    stackParams = {
        "name": name,
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    stack = nodeStackClass(stackParams, msgHandler, registry, randomString(32), config=tconf)
    return stack


@pytest.fixture()
def connected_nodestacks(registry, tdir, looper, connection_timeout, tconf):
    genKeys(tdir, registry.keys())
    stacks = []
    for name, ha in registry.items():
        printer = Printer(name)
        stackParams = dict(name=name, ha=ha, basedirpath=tdir,
                           auth_mode=AuthMode.RESTRICTED.value)
        reg = copy(registry)
        reg.pop(name)
        stack = KITZStack(stackParams, printer.print, reg)
        stack.listenerQuota = tconf.NODE_TO_NODE_STACK_QUOTA
        stack.listenerSize = tconf.NODE_TO_NODE_STACK_SIZE
        stacks.append(stack)

    motors = prepStacks(looper, *stacks, connect=False, useKeys=True)

    looper.run(eventually(
        checkStacksConnected, stacks, retryWait=1, timeout=connection_timeout))

    return stacks, motors


def test_set_quota(tdir, tconf, registry):
    changed_val = 100000
    tconf.NODE_TO_NODE_STACK_QUOTA = changed_val
    stack = create_fake_nodestack(tdir, tconf, registry)
    assert stack.listenerQuota == tconf.NODE_TO_NODE_STACK_QUOTA


def test_set_size(tdir, tconf, registry):
    changed_val = 100000
    tconf.NODE_TO_NODE_STACK_SIZE = changed_val
    stack = create_fake_nodestack(tdir, tconf, registry)
    assert stack.listenerSize == tconf.NODE_TO_NODE_STACK_SIZE


def test_limit_by_msg_count(looper, tdir, tconf, connected_nodestacks):
    stacks, motors = connected_nodestacks
    stackA = stacks[0]
    stackB = stacks[1]
    msg = 'some test messages'
    for i in range(tconf.NODE_TO_NODE_STACK_QUOTA + 10):
        stackA.send(msg, 'Beta')
    received_msgs = stackB._receiveFromListener(Quota(count=stackA.listenerQuota, size=stackA.listenerSize))
    assert received_msgs <= tconf.NODE_TO_NODE_STACK_QUOTA


def test_limit_by_msg_size(looper, tdir, tconf, connected_nodestacks):
    stacks, motors = connected_nodestacks
    stackA = stacks[0]
    stackB = stacks[1]
    msg = 'some test messages'
    limit_size = (tconf.NODE_TO_NODE_STACK_QUOTA - 10) * len(msg)
    stackB.listenerSize = limit_size
    for i in range(tconf.NODE_TO_NODE_STACK_QUOTA + 10):
        stackA.send(msg, 'Beta')
    received_msgs = stackB._receiveFromListener(Quota(count=stackA.listenerQuota, size=stackA.listenerSize))
    assert received_msgs < tconf.NODE_TO_NODE_STACK_QUOTA
