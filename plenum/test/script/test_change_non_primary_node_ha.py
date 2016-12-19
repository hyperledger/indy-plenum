from plenum.test.script.helper import looper, tconf
from plenum.test.script.helper import changeNodeHa
from plenum.common.log import getlogger


logger = getlogger()

whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message']


def testChangeNodeHaForNonPrimary(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                  poolTxnData, poolTxnStewardNames, tconf):
    changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns,
                 poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary=False)