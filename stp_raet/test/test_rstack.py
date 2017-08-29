from stp_core.loop.eventually import eventually
from stp_core.network.auth_mode import AuthMode
from stp_core.network.port_dispenser import genHa
from stp_raet.rstack import SimpleRStack
from stp_core.test.helper import Printer, chkPrinted, prepStacks, checkStacksConnected


def test2RStackCommunication(tdir, looper):
    names = ['Alpha', 'Beta']
    alphaP = Printer(names[0])
    betaP = Printer(names[1])

    stackParamsAlpha = {
        "name": names[0],
        "ha": genHa(),
        "auth_mode": AuthMode.ALLOW_ANY.value,
        "main": True,
        "mutable": "mutable",
        "messageTimeout": 30,
        "basedirpath": tdir
    }
    stackParamsBeta = {
        "name": names[1],
        "ha": genHa(),
        "main": True,
        "auth_mode": AuthMode.ALLOW_ANY.value,
        "mutable": "mutable",
        "messageTimeout": 30,
        "basedirpath": tdir
    }

    alpha = SimpleRStack(stackParamsAlpha, msgHandler=alphaP.print)
    beta = SimpleRStack(stackParamsBeta, msgHandler=betaP.print)

    alpha.connect(ha=beta.ha)
    beta.connect(ha=alpha.ha)

    prepStacks(looper, alpha, beta, connect=False, useKeys=False)

    looper.run(eventually(checkStacksConnected, [alpha, beta], retryWait=1,
                          timeout=10))

    alpha.send({'greetings': 'hi Beta'}, beta.name)
    beta.send({'greetings': 'hi Alpha'}, alpha.name)

    looper.run(eventually(chkPrinted, betaP, {'greetings': 'hi Beta'}))
    looper.run(eventually(chkPrinted, alphaP, {'greetings': 'hi Alpha'}))
