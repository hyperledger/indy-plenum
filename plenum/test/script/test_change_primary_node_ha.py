import pytest

from plenum.test.script.helper import looper, tconf
from stp_core.common.log import getlogger
from plenum.test.script.helper import changeNodeHa


logger = getlogger()

whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message',
             'got error while verifying message']


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-330')
# @pytest.mark.skip(reason="SOV-972")
def testChangeNodeHaForPrimary(looper, txnPoolNodeSet, tdirWithPoolTxns,
                               poolTxnData, poolTxnStewardNames, tconf):
    changeNodeHa(looper,
                 txnPoolNodeSet,
                 tdirWithPoolTxns,
                 poolTxnData,
                 poolTxnStewardNames,
                 tconf,
                 shouldBePrimary=True)
