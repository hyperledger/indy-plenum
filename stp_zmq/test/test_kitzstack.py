from copy import copy

import pytest

from plenum.common.constants import OP_FIELD_NAME, BATCH
from plenum.common.stacks import nodeStackClass
from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, prepStacks, \
    checkStacksConnected, connectStack
from stp_zmq.test.helper import genKeys
from stp_zmq.kit_zstack import KITZStack


def testKitZStacksConnected(registry, tdir, looper, tconf):
    genKeys(tdir, registry.keys())
    stacks = []
    for name, ha in registry.items():
        printer = Printer(name)
        stackParams = dict(name=name, ha=ha, basedirpath=tdir,
                           auth_mode=AuthMode.RESTRICTED.value)
        reg = copy(registry)
        reg.pop(name)
        stack = KITZStack(stackParams, printer.print, reg)
        stacks.append(stack)

    prepStacks(looper, *stacks, connect=False, useKeys=True)
    # TODO: the connection may not be established for the first try because
    # some of the stacks may not have had a remote yet (that is they haven't had yet called connect)
    timeout = 2 * tconf.RETRY_TIMEOUT_RESTRICTED + 1
    looper.run(eventually(
        checkStacksConnected, stacks, retryWait=1, timeout=timeout))


@pytest.fixture(scope="function")
def func_create_stacks(tdir, registry):
    def create_stack(count):
        genKeys(tdir, registry.keys())
        stacks = []
        for name, ha in registry.items():
            printer = Printer(name)
            stackParams = dict(name=name, ha=ha, basedirpath=tdir,
                               auth_mode=AuthMode.RESTRICTED.value)
            reg = copy(registry)
            reg.pop(name)
            stack = nodeStackClass(stackParams, printer.print, reg)
            stack.start()
            stacks.append(stack)
            if len(stacks) == count:
                break
        return stacks

    yield create_stack


def test_use_send_from_zstack_on_resend(func_create_stacks, looper):
    aStack, bStack = func_create_stacks(2)
    connectStack(aStack, bStack)
    """
    Sending some pi msgs for creating a batch on flashOutBox
    This function just put 'pi ' message into outBoxes queue, not send
    """
    aStack.sendPingPong(bStack.name)
    aStack.sendPingPong(bStack.name)
    """
    Emulate batch creation and sending. Batch should be added into _stashed_to_disconnected queue
    """
    aStack.flushOutBoxes()
    assert len(aStack._stashed_to_disconnected[bStack.name]) == 1
    batch = aStack.deserializeMsg(aStack._stashed_to_disconnected[bStack.name][0])
    assert OP_FIELD_NAME in batch and batch[OP_FIELD_NAME] == BATCH

    """
    This method call connect method for bStack and put 'pi' message into outBoxes queue
    """
    connectStack(bStack, aStack)
    """
    Wait for socket's connecting routines
    """
    looper.runFor(1)
    """
    This instruction get 'pi' message from outBoxes queue, create a batch if needed and send it to aStack
    """
    bStack.flushOutBoxes()
    """
    It needs for getting 'pi' message from bStack. It process 'pi' message and put 'po' message to outBoxes queue 
    """
    looper.run(aStack.service())
    """
    Send 'po' message to bStack
    """
    aStack.flushOutBoxes()
    """
    Processing previous sending batch (zmq feature) and 'po'
    """
    looper.run(bStack.service())
    """
    For sending 'po' message to aStack
    """
    bStack.flushOutBoxes()

    """
    Append 'pi' msg for checking that batch into batch will not be included
    """
    aStack._stashed_to_disconnected[bStack.name].append('pi')
    """
    Emulate that aStack got 'po' message from bStack and it must run _resend_to_disconnected
    """
    looper.run(aStack.service())
    """
    Emulate batch creating and sending
    """
    aStack.flushOutBoxes()

    looper.run(bStack._serviceStack(bStack.age, None))
    assert len(bStack.rxMsgs) == 2
    """
    rxMsgs queue should contains only one 'pi' message from step 3 and batch 
    which was failed to sending to disconnected stack from step 2
    """
    got_pi = False
    got_batch = False
    while bStack.rxMsgs:
        m, frm = bStack.rxMsgs.popleft()
        if m.encode() not in bStack.healthMessages:
            msg = bStack.deserializeMsg(m)
        else:
            got_pi = True
            continue
        if OP_FIELD_NAME in msg and msg[OP_FIELD_NAME] == BATCH:
            got_batch = True
            continue
    assert got_pi and got_batch
