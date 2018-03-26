import os

from jsonpickle import json
from plenum.client.client import Client
from plenum.client.wallet import Wallet
from plenum.common.constants import TXN_TYPE, TARGET_NYM, DATA, NODE_IP, \
    NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS, NODE, CLIENT_STACK_SUFFIX, SERVICES, VALIDATOR
from plenum.common.roles import Roles
from plenum.common.signer_did import DidSigner
from plenum.common.signer_simple import SimpleSigner
from plenum.common.transactions import PlenumTransactions
from plenum.test import waits
from storage.text_file_store import TextFileStore
from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.types import HA

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


def submitNodeIpChange(client, stewardWallet, name: str, nym: str,
                       nodeStackHa: HA, clientStackHa: HA):
    (nodeIp, nodePort), (clientIp, clientPort) = nodeStackHa, clientStackHa
    txn = {
        TXN_TYPE: NODE,
        TARGET_NYM: nym,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: int(nodePort),
            CLIENT_IP: clientIp,
            CLIENT_PORT: int(clientPort),
            ALIAS: name,
            SERVICES: [VALIDATOR],
        }
    }
    signedOp = stewardWallet.signOp(txn, stewardWallet.defaultId)
    req, _ = client.submitReqs(signedOp)
    return req[0]


def __checkClientConnected(cli, ):
    assert cli.hasSufficientConnections


def changeHA(looper, config, nodeName, nodeSeed, newNodeHA,
             stewardName, stewardsSeed, newClientHA=None, basedir=None):
    if not newClientHA:
        newClientHA = HA(newNodeHA.host, newNodeHA.port + 1)

    assert basedir is not None

    # prepare steward wallet
    stewardSigner = DidSigner(seed=stewardsSeed)
    stewardWallet = Wallet(stewardName)
    stewardWallet.addIdentifier(signer=stewardSigner)

    # prepare client to submit change ha request
    _, randomClientPort = genHa()
    client = Client(stewardName, ha=('0.0.0.0', randomClientPort), config=config, basedirpath=basedir)
    looper.add(client)
    timeout = waits.expectedClientToPoolConnectionTimeout(4)
    looper.run(eventually(__checkClientConnected, client,
                          retryWait=1, timeout=timeout))

    nodeVerKey = SimpleSigner(seed=nodeSeed).verkey

    # send request
    req = submitNodeIpChange(client, stewardWallet, nodeName, nodeVerKey,
                             newNodeHA, newClientHA)
    return client, req
