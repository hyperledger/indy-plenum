import os

from jsonpickle import json

from plenum.common.constants import CLIENT_STACK_SUFFIX
from plenum.common.roles import Roles
from plenum.common.transactions import PlenumTransactions
from storage.text_file_store import TextFileStore

NodeInfoFile = "node-info"
GenTxnFile = "genesis_txn"
ExportedTxnFile = "exported_genesis_txn"


def buildKeepDirIfNotExists(baseDir):
    keepDir = os.path.expanduser(baseDir)
    if not os.path.exists(keepDir):
        os.makedirs(keepDir, exist_ok=True)


def isNodeType(baseDir, name):
    filepath = os.path.join(os.path.expanduser(baseDir),
                            name + CLIENT_STACK_SUFFIX)
    if os.path.exists(filepath):
        return True
    else:
        return False


def getLedger(baseDir, dbName, storeHash=True, isLineNoKey: bool = False):
    return TextFileStore(
        dbDir=baseDir,
        dbName=dbName,
        storeContentHash=storeHash,
        isLineNoKey=isLineNoKey)


def storeToFile(baseDir, dbName, value, key,
                storeHash=True, isLineNoKey=False):
    ledger = getLedger(baseDir, dbName, storeHash=storeHash,
                       isLineNoKey=isLineNoKey)
    if key is None:
        ledger.put(value)
    else:
        ledger.put(value, key)
    ledger.close()


def getNodeInfo(baseDir, nodeName):
    ledger = getLedger(baseDir, NodeInfoFile, storeHash=False,
                       isLineNoKey=False)
    rec = ledger.get(nodeName)
    ledger.close()
    return json.loads(rec)


def storeNodeInfo(baseDir, nodeName, steward, nodeip, nodeport, clientip,
                  clientport):
    data = {}
    vnodeip, vnodeport, vclientip, vclientport = getAddGenesisHAs(nodeip,
                                                                  nodeport,
                                                                  clientip,
                                                                  clientport)
    nodeAddr = vnodeip + ":" + str(vnodeport)
    clientAddr = vclientip + ":" + str(vclientport)
    data['steward'] = steward
    data['nodeAddr'] = nodeAddr
    data['clientAddr'] = clientAddr
    newJsonData = json.dumps(data)

    ledger = getLedger(baseDir, NodeInfoFile, storeHash=False,
                       isLineNoKey=False)
    storedJsonData = ledger.get(nodeName)
    if not storedJsonData:
        storeToFile(baseDir, NodeInfoFile, newJsonData, nodeName,
                    storeHash=False, isLineNoKey=False)
    elif not storedJsonData == newJsonData:
        newRec = []
        for key, jsonValue in ledger.iterator(include_key=True,
                                              include_value=True):
            if key != nodeName:
                newRec.append((key, jsonValue))
        newRec.append((nodeName, newJsonData))
        ledger.reset()
        for key, value in newRec:
            storeToFile(baseDir, NodeInfoFile, value, key, storeHash=False,
                        isLineNoKey=False)
    ledger.close()


def storeExportedTxns(baseDir, txn):
    storeToFile(baseDir, ExportedTxnFile, txn, None, storeHash=False,
                isLineNoKey=True)


def storeGenTxns(baseDir, txn):
    storeToFile(baseDir, GenTxnFile, txn, None, storeHash=False,
                isLineNoKey=True)


def getAddGenesisHAs(nodeip, nodeport, clientip, clientport):
    vnodeip = nodeip if nodeip else "127.0.0.1"
    vnodeport = nodeport if nodeport else "9701"
    vclientip = clientip if clientip else vnodeip
    vclientport = clientport if clientport else str(int(vnodeport) + 1)
    return vnodeip, vnodeport, vclientip, vclientport


def getAddNewGenNodeCommand(name, verkey, stewardkey, nodeip, nodeport,
                            clientip, clientport):
    vnodeip, vnodeport, vclientip, vclientport = getAddGenesisHAs(nodeip,
                                                                  nodeport,
                                                                  clientip,
                                                                  clientport)
    nodeAddr = vnodeip + ":" + vnodeport
    clientAddr = vclientip + ":" + vclientport

    return 'add genesis transaction {node} with data {"'.format(node=PlenumTransactions.NODE.name) + name + '": {' \
                                                                                                            '"verkey": ' + verkey + \
           '"node_address": "' + nodeAddr + '", "client_address": "' + \
           clientAddr + '"},' \
                        '"by": "' + stewardkey + '"}'


def getOldAddNewGenNodeCommand(name, verkey, stewardverkey, nodeip, nodeport,
                               clientip, clientport):
    vnodeip, vnodeport, vclientip, vclientport = getAddGenesisHAs(nodeip,
                                                                  nodeport,
                                                                  clientip,
                                                                  clientport)
    return 'add genesis transaction {node} for '.format(node=PlenumTransactions.NODE.name) + verkey + ' by ' + \
           stewardverkey + ' with data {"node_ip": "' + \
           vnodeip + '", "node_port": ' + vnodeport + ', "client_ip": "' + \
           vclientip + '", "client_port": ' + \
           vclientport + ', "alias": "' + name + '"}'


def generateNodeGenesisTxn(baseDir, displayTxn, name, verkey, stewardverkey,
                           nodeip, nodeport, clientip, clientport):
    storeNodeInfo(baseDir, name, stewardverkey, nodeip, nodeport, clientip,
                  clientport)
    txn = getOldAddNewGenNodeCommand(name, verkey, stewardverkey, nodeip,
                                     nodeport, clientip, clientport)
    storeGenTxns(baseDir, txn)
    printGenTxn(txn, displayTxn)


def getAddNewGenStewardCommand(name, verkey):
    return 'add genesis transaction {nym} with data {"'.format(nym=PlenumTransactions.NYM.name) \
           + name + '": {"verkey": "' + verkey + \
           '"} role={role}'.format(role=Roles.STEWARD.name)


def getOldAddNewGenStewardCommand(name, verkey):
    return 'add genesis transaction {nym} for '.format(nym=PlenumTransactions.NYM.name) + verkey + ' with data ' \
                                                                                                   '{"alias": ' \
                                                                                                   '"' + name + \
           '"} role={role}'.format(role=Roles.STEWARD.name)


def generateStewardGenesisTxn(baseDir, displayTxn, name, verkey):
    txn = getOldAddNewGenStewardCommand(name, verkey)
    storeGenTxns(baseDir, txn)
    printGenTxn(txn, displayTxn)


def printGenTxn(txn, displayTxn):
    if displayTxn:
        print('\n' + txn)


def _checkClientConnected(cli, ):
    assert cli.hasSufficientConnections
