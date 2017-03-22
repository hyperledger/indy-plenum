from copy import copy

from stp_core.zmq.zstack import KITZStack

from plenum.common.eventually import eventually
from plenum.test.zstack_tests.helper import genKeys, Printer, prepStacks, \
    checkStacksConnected


def testKitZStacksConnected(registry, tdir, looper):
    genKeys(tdir, registry.keys())
    stacks = []
    for name, ha in registry.items():
        printer = Printer(name)
        stackParams = dict(name=name, ha=ha, basedirpath=tdir, auto=0)
        reg = copy(registry)
        reg.pop(name)
        stack = KITZStack(stackParams, printer.print, reg)
        stacks.append(stack)

    prepStacks(looper, *stacks, connect=False)
    looper.run(eventually(checkStacksConnected, stacks, retryWait=1, timeout=10))

