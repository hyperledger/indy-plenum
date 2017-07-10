import time
from typing import NamedTuple
from typing import Optional

from raet.raeting import TrnsKind, PcktKind


RaetDelay = NamedTuple("RaetDelay", [
    ("tk", Optional[TrnsKind]),
    ("pk", Optional[PcktKind]),
    ("fromPort", Optional[int])])


def handshake(*stacks):
    svc(stacks)
    print("Finished Handshake\n")


def svc(stacks):
    while True:
        for stack in stacks:
            stack.serviceAll()
            stack.store.advanceStamp(0.1)
        if all([not stack.transactions for stack in stacks]):
            break
        time.sleep(.1)


def cleanup(*stacks):
    for stack in stacks:
        stack.server.close()  # close the UDP socket
        stack.keep.clearAllDir()  # clear persisted data
    print("Finished\n")


def sendMsgs(frm, to, toRemote):
    stacks = [frm, to]
    msg = {'subject': 'Example message {} to {}'.format(frm.name, to.name),
           'content': 'test'}
    frm.transmit(msg, toRemote.uid)
    svc(stacks)
    rx = to.rxMsgs.popleft()
    print("{0}\n".format(rx))
    print("Finished Message {} to {}\n".format(frm.name, to.name))
    msg = {'subject': 'Example message {} to {}'.format(to.name, frm.name),
           'content': 'Another test.'}
    to.transmit(msg, toRemote.uid)
    svc(stacks)
    rx = frm.rxMsgs.popleft()
    print("{0}\n".format(rx))
    print("Finished Message {} to {}\n".format(to.name, frm.name))


def getRemote(stack, name):
    return next(r for r in stack.remotes.values() if r.name == name)
