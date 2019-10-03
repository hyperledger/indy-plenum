from copy import copy
from itertools import combinations

import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.test.helper import Printer, prepStacks, \
    checkStacksConnected, chkPrinted
from stp_zmq.test.helper import genKeys
from stp_zmq.kit_zstack import KITZStack


@pytest.mark.skip('Use a packet capture tool')
def testKitZStacksCommunication(registry, tdir, looper):
    # TODO: Use a packet capture tool
    genKeys(tdir, registry.keys())
    stacks = []
    names = []
    for name, ha in registry.items():
        printer = Printer(name)
        stackParams = dict(name=name, ha=ha, basedirpath=tdir,
                           auth_mode=AuthMode.RESTRICTED.value)
        reg = copy(registry)
        reg.pop(name)
        stack = KITZStack(stackParams, printer.print, reg)
        stacks.append(stack)
        names.append((name, printer))

    prepStacks(looper, *stacks, connect=False, useKeys=True)
    # TODO: the connection may not be established for the first try because
    # some of the stacks may not have had a remote yet (that is they haven't had yet called connect)
    timeout = 4 * KITZStack.RETRY_TIMEOUT_RESTRICTED + 1
    looper.run(eventually(
        checkStacksConnected, stacks, retryWait=1, timeout=timeout))

    def send_recv():
        for i, j in combinations(range(len(stacks)), 2):
            alpha = stacks[i]
            beta = stacks[j]

            alpha.send({'greetings': 'hi'}, beta.name)
            beta.send({'greetings': 'hello'}, alpha.name)

            looper.run(
                eventually(chkPrinted, names[i][1], {'greetings': 'hello'}, timeout=15))
            looper.run(eventually(chkPrinted, names[j][1], {
                       'greetings': 'hi'}, timeout=15))

            names[i][1].reset()
            names[j][1].reset()

    def pr():
        for stack in stacks:
            for r in stack.remotes.values():
                print(r._get_monitor_events())

    for _ in range(100):
        # send_recv()
        looper.run(eventually(
            checkStacksConnected, stacks, retryWait=1, timeout=timeout))
        # pr()
        looper.runFor(30)
        # pr()
