from ledger.stores.text_file_store import TextFileStore
from plenum.common.raet import initLocalKeep, getEncodedLocalVerKey, getLocalVerKey

NodeStewardMappingFile = "node-steward-mapping"
GenTxnFile = "genesis_txn"

def storeToFile(baseDir, dbName, value, key, storeHash=True, isLineNoKey: bool=False):
    ledger = TextFileStore(
        dbDir = baseDir,
        dbName = dbName,
        storeContentHash = storeHash,
        isLineNoKey = isLineNoKey)
    if key is None:
        ledger.put(value)
    else:
        ledger.put(value, key)


def storeNodeStewardMapping(baseDir, nodeName, stewardName):
    storeToFile(baseDir, NodeStewardMappingFile, stewardName, nodeName, storeHash=False, isLineNoKey=False)

def storeGenTxns(baseDir, txn):
    storeToFile(baseDir, GenTxnFile, txn, None, storeHash=False, isLineNoKey=True)

def initKeep(name, baseDir, pkseed, sigseed, override=False):
    pubkey, verkey = initLocalKeep(name, baseDir, pkseed, sigseed, override)
    print("Public key is", pubkey)
    print("Verification key is", verkey)
    return (pubkey, verkey)

def getStewardKeyFromName(baseDir, name):
    return getLocalVerKey(name, baseDir)


def printNodeGenesisTrans(baseDir, name, verkey, pubkey, vstewardverkey, nodeip, nodeport, clientip, clientport):
    vnodeip = nodeip if nodeip else "127.0.0.1"
    vnodeport = nodeport if nodeport else "9701"
    vclientip = clientip if clientip else vnodeip
    vclientport = clientport if clientport else str(int(vnodeport)+1)

    txn = 'add genesis transaction NEW_NODE for ' + verkey + ' by ' + vstewardverkey + ' with data {"node_ip": "' + vnodeip + '", "node_port": ' + vnodeport + ', "client_ip": "{}", "client_port": ' + vclientport + ', "pubkey": "' + pubkey + '", "alias": "' + name + '"}'

    storeGenTxns(baseDir, txn)
    print(txn)


def printStewardGenesisTrans(baseDir, name, verkey, pubkey):
    txn = 'add genesis transaction NEW_STEWARD for ' + verkey + ' with data {"alias": "' + name + '", "pubkey": "' + pubkey + '"}'
    storeGenTxns(baseDir, txn)
    print(txn)