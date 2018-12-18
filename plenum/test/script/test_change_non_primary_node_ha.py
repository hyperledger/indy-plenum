import pytest

from stp_core.common.log import getlogger
from plenum.test.script.helper import changeNodeHa

logger = getlogger()

whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message',
             'got error while verifying message']

TestRunningTimeLimitSec = 200

@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-330')
def testChangeNodeHaForNonPrimary(looper, txnPoolNodeSet,
                                  poolTxnData, poolTxnStewardNames, tconf, tdir,
                                  sdk_pool_handle, sdk_wallet_stewards,
                                  sdk_wallet_client):
    changeNodeHa(looper,
                 txnPoolNodeSet,
                 tconf,
                 shouldBePrimary=False,
                 tdir=tdir,
                 sdk_pool_handle=sdk_pool_handle,
                 sdk_wallet_stewards=sdk_wallet_stewards,
                 sdk_wallet_client=sdk_wallet_client)
