from copy import copy

import pytest

from plenum.common.constants import OP_FIELD_NAME, BATCH
from plenum.common.messages.node_messages import Batch
from plenum.common.stacks import nodeStackClass
from plenum.common.types import f
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, connectStack
from stp_zmq.test.helper import genKeys


@pytest.fixture()
def registry():
    return {
        'Alpha': genHa(),
        'Beta': genHa(),
        'Gamma': genHa(),
        'Delta': genHa()
    }


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
    batch_to_disconnected = aStack.deserializeMsg(aStack._stashed_to_disconnected[bStack.name][0])
    assert OP_FIELD_NAME in batch_to_disconnected and batch_to_disconnected[OP_FIELD_NAME] == BATCH

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
            if msg == batch_to_disconnected:
                """
                Exactly the same batch which should be sent to disconnected node
                """
                got_batch = True
                continue
            else:
                """Check that there is no batches with batch as message"""
                batch = Batch(messages=msg[f.MSGS.nm],
                              signature=msg[f.SIG.nm])
                for m in batch.messages:
                    assert OP_FIELD_NAME not in m and BATCH not in m
    assert got_pi and got_batch
