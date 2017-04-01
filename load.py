from plenum.client.client import *
from plenum.client.wallet import *
from stp_core.network.port_dispenser import genHa
from stp_core.loop.looper import Looper
from plenum.test.helper import *
from time import *
from plenum.common.signer_simple import SimpleSigner

numReqs = 500
splits = 1


def load():
    port = genHa()[1]
    ha = HA('0.0.0.0', port)
    name = "hello"
    wallet = Wallet(name)
    wallet.addIdentifier(
        signer=SimpleSigner(seed=b'000000000000000000000000Steward1'))
    client = Client(name, ha=ha)
    with Looper(debug=True) as looper:
        looper.add(client)
        requests = sendRandomRequests(wallet, client, numReqs)
        print('Sending {} reqs'.format(numReqs))
        start = perf_counter()
        for i in range(0, numReqs, numReqs // splits):
            checkSufficientRepliesForRequests(looper, client, requests[
                                                              i:i + numReqs // splits + 1],
                                              2, 3)
            print('>>> Got replies for {} requests << in {}'.
                  format(numReqs // splits, time.perf_counter() - start))
        end = perf_counter()
        print('>>>{}<<<'.format(end - start))
        exit(0)


if __name__ == "__main__":
    load()
