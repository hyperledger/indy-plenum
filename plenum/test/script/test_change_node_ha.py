# TODO: This is failing as of now, fix it
# def testStopScriptIfNodeIsRunning(looper, txnPoolNodeSet, poolTxnData,
#                                   poolTxnStewardData, tconf):
#     nodeName = txnPoolNodeSet[0].name
#     nodeSeed = poolTxnData["seeds"][nodeName].encode()
#     stewardName, stewardsSeed = poolTxnStewardData
#     ip, port = genHa()
#     nodeStackNewHA = HA(ip, port)
#
#     # the node `nodeName` is not stopped here
#
#     # change HA
#     with pytest.raises(Exception, message="Node '{}' must be stopped "
#                                           "before".format(nodeName)):
#         changeHA(looper, tconf, nodeName, nodeSeed, nodeStackNewHA,
#                  stewardName, stewardsSeed)


