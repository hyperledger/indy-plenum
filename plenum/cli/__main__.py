import sys
from collections import OrderedDict
from tempfile import TemporaryDirectory

from plenum.cli.cli import Cli
from plenum.common.looper import Looper
from plenum.common.util import getConfig


def main(logfile: str=None, debug=None, cliClass=None):
    # nodeReg = OrderedDict([
    #     ('Alpha', (('127.0.0.1', 8001), )),
    #     ('Beta', (('127.0.0.1', 8003), )),
    #     ('Gamma', (('127.0.0.1', 8005), )),
    #     ('Delta', (('127.0.0.1', 8007), ))
    # ])
    #
    # cliNodeReg = OrderedDict([
    #     ('AlphaC', (('127.0.0.1', 8002), )),
    #     ('BetaC', (('127.0.0.1', 8004), )),
    #     ('GammaC', (('127.0.0.1', 8006), )),
    #     ('DeltaC', (('127.0.0.1', 8008), ))
    #     ])

    config = getConfig()
    nodeReg = config.nodeReg
    cliNodeReg = config.cliNodeReg

    if not cliClass:
        cliClass = Cli

    with Looper(debug=False) as looper:
        with TemporaryDirectory() as tmpdir:
            cli = cliClass(looper=looper,
                           tmpdir=tmpdir,
                           nodeReg=nodeReg,
                           cliNodeReg=cliNodeReg,
                           logfile=logfile,
                           debug=debug)

            if not debug:
                looper.run(cli.shell(*sys.argv[1:]))
                print('Goodbye.')
            return cli

if __name__ == '__main__':
    main()
