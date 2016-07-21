import os

import pytest


@pytest.fixture("module")
def loadBankReqPlugin(cli):
    # TODO: Remove this boilerplate
    curPath = os.path.dirname(os.path.dirname(__file__))
    fullPath = os.path.join(curPath, 'plugin', 'bank_req_validation')
    cli.enterCmd("load plugins from {}".format(fullPath))
    fullPath = os.path.join(curPath, 'plugin', 'bank_req_processor')
    cli.enterCmd("load plugins from {}".format(fullPath))
    # TODO: Remove this hardcoded sleep
    cli.looper.runFor(2)


def testWithBankReqPluginLoaded(cli, loadBankReqPlugin, createAllNodes):
    def chk():
        assert cli.lastCmdOutput == "No such client. See: 'help new' for more details"

    cli.enterCmd("client Random balance")
    chk()
    cli.enterCmd("client Random credit 400 to RandomNew")
    chk()
    cli.enterCmd("client Random transactions")
    chk()
