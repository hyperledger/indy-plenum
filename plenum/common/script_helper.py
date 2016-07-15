from jsonpickle import json

from ledger.stores.text_file_store import TextFileStore
from plenum.common.raet import initLocalKeep, getEncodedLocalVerKey, getLocalVerKey

NodeInfoFile = "node-info.txt"
GenTxnFile = "genesis_txn"


def getLedger(baseDir, dbName, storeHash=True, isLineNoKey: bool=False):
    return TextFileStore(
        dbDir = baseDir,
        dbName = dbName,
        storeContentHash = storeHash,
        isLineNoKey = isLineNoKey)


def storeToFile(baseDir, dbName, value, key, storeHash=True, isLineNoKey=False):
    ledger = getLedger(baseDir, dbName, storeHash=storeHash, isLineNoKey=isLineNoKey)
    if key is None:
        ledger.put(value)
    else:
        ledger.put(value, key)


def getNodeInfo(baseDir, nodeName):
    ledger = getLedger(baseDir, NodeInfoFile, storeHash=False, isLineNoKey=False)
    rec = ledger.get(nodeName)
    return json.loads(rec)


def storeNodeInfo(baseDir, nodeName, steward, nodeip, nodeport, clientip, clientport):
    data = {}
    vnodeip, vnodeport, vclientip, vclientport = getAddGenesisHAs(nodeip, nodeport, clientip, clientport)
    nodeAddr = vnodeip + ":" + str(vnodeport)
    clientAddr = vclientip + ":" + str(vclientport)
    data['steward'] = steward
    data['nodeAddr'] = nodeAddr
    data['clientAddr'] = clientAddr
    newJsonData = json.dumps(data)

    ledger = getLedger(baseDir, NodeInfoFile, storeHash=False, isLineNoKey=False)
    storedJsonData = ledger.get(nodeName)
    if not storedJsonData:
        storeToFile(baseDir, NodeInfoFile, newJsonData, nodeName, storeHash=False, isLineNoKey=False)
    elif not storedJsonData == newJsonData:
        newRec = []
        for key, jsonValue in ledger.iterator(includeKey=True, includeValue=True):
            if key != nodeName:
                newRec.append((key, jsonValue))
        newRec.append((nodeName, newJsonData))
        ledger.reset()
        for key, value in newRec:
            storeToFile(baseDir, NodeInfoFile, value, key, storeHash=False, isLineNoKey=False)


def storeGenTxns(baseDir, txn):
    storeToFile(baseDir, GenTxnFile, txn, None, storeHash=False, isLineNoKey=True)


def initKeep(baseDir, name, pkseed, sigseed, override=False):
    pubkey, verkey = initLocalKeep(name, baseDir, pkseed, sigseed, override)
    print("Public key is", pubkey)
    print("Verification key is", verkey)
    return (pubkey, verkey)


def getStewardKeyFromName(baseDir, name):
    return getLocalVerKey(name, baseDir)

def getAddGenesisHAs(nodeip, nodeport, clientip, clientport):
    vnodeip = nodeip if nodeip else "127.0.0.1"
    vnodeport = nodeport if nodeport else "9701"
    vclientip = clientip if clientip else vnodeip
    vclientport = clientport if clientport else str(int(vnodeport) + 1)
    return (vnodeip, vnodeport, vclientip, vclientport)

def getAddGenesisNewNodeCommand(name, verkey, pubkey, stewardverkey, nodeip, nodeport, clientip, clientport):
    vnodeip, vnodeport, vclientip, vclientport = getAddGenesisHAs(nodeip, nodeport, clientip, clientport)

    return 'add genesis transaction NEW_NODE for ' + verkey + ' by ' + stewardverkey + ' with data {"node_ip": "' + \
    vnodeip + '", "node_port": ' + vnodeport + ', "client_ip": "' + vclientip + '", "client_port": ' + \
    vclientport + ', "pubkey": "' + pubkey + '", "alias": "' + name + '"}'

def storeAndPrintNodeGenesisTrans(baseDir, name, verkey, pubkey, stewardverkey, nodeip, nodeport, clientip, clientport):
    txn = getAddGenesisNewNodeCommand(name, verkey, pubkey, stewardverkey, nodeip, nodeport, clientip, clientport)

    storeNodeInfo(baseDir, name, stewardverkey, nodeip, nodeport, clientip, clientport)
    storeGenTxns(baseDir, txn)
    print(txn)

def getAddGenesisNewStewardCommand(name, verkey, pubkey):
    return 'add genesis transaction NEW_STEWARD for ' + verkey + ' with data {"alias": "' + name + '", "pubkey": "' + pubkey + '"}'

def printStewardGenesisTrans(baseDir, name, verkey, pubkey):
    txn = getAddGenesisNewStewardCommand(name, verkey, pubkey)
    storeGenTxns(baseDir, txn)
    print(txn)