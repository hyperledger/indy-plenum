from copy import copy

import pytest
from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_raet.rstack import KITRStack
from stp_core.test.helper import Printer, prepStacks, checkStacksConnected, chkPrinted


@pytest.fixture()
def printers(registry):
    printersDict = {}
    for name, ha in registry.items():
        printersDict[name] = Printer(name)
    return printersDict


@pytest.fixture()
def stacks(registry, tdir, looper, printers):
    rstacks = []
    for name, ha in registry.items():
        printer = printers[name]
        stackParams = {
            "name": name,
            "ha": ha,
            "auth_mode": AuthMode.ALLOW_ANY.value,
            "main": True,
            "mutable": "mutable",
            "messageTimeout": 30,
            "basedirpath": tdir
        }
        reg = copy(registry)
        reg.pop(name)
        stack = KITRStack(stackParams, printer.print, reg)
        rstacks.append(stack)
    prepStacks(looper, *rstacks, connect=True, useKeys=False)
    return rstacks


def testKitRStacksConnected(looper, stacks):
    looper.run(eventually(checkStacksConnected, stacks, retryWait=1,
                          timeout=10))


def testKitRStacksSendMesages(looper, stacks, printers):
    looper.run(eventually(checkStacksConnected, stacks, retryWait=1,
                          timeout=10))

    stacks[0].send({'greetings': 'hi'}, stacks[1].name)

    looper.run(eventually(
        chkPrinted, printers[stacks[1].name], {'greetings': 'hi'}))
