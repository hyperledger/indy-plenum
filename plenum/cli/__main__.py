import sys

from plenum.cli.cli import Cli
from stp_core.loop.looper import Looper
from plenum.common.config_util import getConfig


def main(logfile: str=None, debug=None, cliClass=None):
    config = getConfig()
    nodeReg = config.nodeReg
    cliNodeReg = config.cliNodeReg
    basedirpath = config.baseDir

    if not cliClass:
        cliClass = Cli

    with Looper(debug=config.LOOPER_DEBUG) as looper:
        cli = cliClass(looper=looper,
                       basedirpath=basedirpath,
                       nodeReg=nodeReg,
                       cliNodeReg=cliNodeReg,
                       logFileName=logfile,
                       debug=debug)

        if not debug:
            looper.run(cli.shell(*sys.argv[1:]))
            print('Goodbye.')
        return cli


if __name__ == '__main__':
    main()
