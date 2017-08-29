import raet
import time
from raet.raeting import AutoMode


def example2():
    alpha = raet.road.stacking.RoadStack(name='alpha',
                                         ha=('0.0.0.0', 7531),
                                         auto=AutoMode.always)

    beta = raet.road.stacking.RoadStack(name='beta',
                                        ha=('0.0.0.0', 7532),
                                        main=True,
                                        auto=AutoMode.always)

    remote = raet.road.estating.RemoteEstate(stack=alpha,
                                             ha=beta.ha)

    alpha.addRemote(remote)

    alpha.join(uid=remote.uid, cascade=True)

    stacks = [alpha, beta]
    while True:
        for stack in stacks:
            stack.serviceAll()
            stack.store.advanceStamp(0.1)
        if all([not stack.transactions for stack in stacks]):
            break
        time.sleep(0.1)

    print("Finished Handshake\n")

    msg = {
        'subject': 'Example message alpha to beta',
        'content': 'The dict keys in this dict are not special any dict will do.',
    }

    alpha.transmit(msg, remote.uid)
    while True:
        for stack in stacks:
            stack.serviceAll()
            stack.store.advanceStamp(0.1)
        if all([not stack.transactions for stack in stacks]):
            break
        time.sleep(0.1)

    rx = beta.rxMsgs.popleft()
    print("{0}\n".format(rx))
    print("Finished Message alpha to beta\n")

    msg = {'subject': 'Example message beta to alpha',
           'content': 'Messages are the core of raet.', }

    beta.transmit(msg, remote.uid)
    while True:
        for stack in stacks:
            stack.serviceAll()
            stack.store.advanceStamp(0.1)
        if all([not stack.transactions for stack in stacks]):
            break
        time.sleep(0.1)

    rx = alpha.rxMsgs.popleft()
    print("{0}\n".format(rx))
    print("Finished Message beta to alpha\n")

    beta.server.close()
    for i in range(10):
        alpha.transmit(msg, remote.uid)
    time.sleep(3)
    for i in range(500):
        alpha.serviceAll()
        alpha.store.advanceStamp(0.1)
        if all([not stack.transactions for stack in stacks]):
            break
        time.sleep(0.1)

    for stack in stacks:
        stack.server.close()  # close the UDP socket
        stack.keep.clearAllDir()  # clear persisted data

    print("Finished\n")


example2()
