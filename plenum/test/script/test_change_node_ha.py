import pytest
from stp_core.network.port_dispenser import genHa
from stp_core.types import HA

from plenum.common.script_helper import changeHA


@pytest.mark.skip(reason='INDY-99')
def testStopScriptIfNodeIsRunning(looper, txnPoolNodeSet, poolTxnData,
                                  poolTxnStewardData, tconf):
    nodeName = txnPoolNodeSet[0].name
    nodeSeed = poolTxnData["seeds"][nodeName].encode()
    stewardName, stewardsSeed = poolTxnStewardData
    ip, port = genHa()
    nodeStackNewHA = HA(ip, port)

    # the node `nodeName` is not stopped here

    # change HA
    with pytest.raises(Exception, message="Node '{}' must be stopped "
                                          "before".format(nodeName)):
        changeHA(looper, tconf, nodeName, nodeSeed, nodeStackNewHA,
                 stewardName, stewardsSeed)
