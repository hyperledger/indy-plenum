import argparse
import os
from ast import literal_eval

from jsonpickle import json

from ledger.stores.text_file_store import TextFileStore
from plenum.cli.constants import ENVS
from plenum.client.client import Client
from plenum.client.wallet import Wallet
from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.raet import initLocalKeep, getLocalVerKey, getLocalPubKey
from plenum.common.signer_simple import SimpleSigner
from plenum.common.txn import TXN_TYPE, CHANGE_HA, TARGET_NYM, DATA, NODE_IP, \
    NODE_PORT, CLIENT_IP, CLIENT_PORT, ALIAS, NEW_NODE
from plenum.common.types import CLIENT_STACK_SUFFIX, HA
from plenum.common.util import getConfig, getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd

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


def storeToFile(baseDir, dbName, value, key, storeHash=True, isLineNoKey=False):
    ledger = getLedger(baseDir, dbName, storeHash=storeHash,
                       isLineNoKey=isLineNoKey)
    if key is None:
        ledger.put(value)
    else:
        ledger.put(value, key)


def getNodeInfo(baseDir, nodeName):
    ledger = getLedger(baseDir, NodeInfoFile, storeHash=False,
                       isLineNoKey=False)
    rec = ledger.get(nodeName)
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
        for key, jsonValue in ledger.iterator(includeKey=True,
                                              includeValue=True):
            if key != nodeName:
                newRec.append((key, jsonValue))
        newRec.append((nodeName, newJsonData))
        ledger.reset()
        for key, value in newRec:
            storeToFile(baseDir, NodeInfoFile, value, key, storeHash=False,
                        isLineNoKey=False)


def storeExportedTxns(baseDir, txn):
    storeToFile(baseDir, ExportedTxnFile, txn, None, storeHash=False,
                isLineNoKey=True)


def storeGenTxns(baseDir, txn):
    storeToFile(baseDir, GenTxnFile, txn, None, storeHash=False,
                isLineNoKey=True)


def initKeep(baseDir, name, sigseed, override=False):
    pubkey, verkey = initLocalKeep(name, baseDir, sigseed, override)
    print("Public key is", pubkey)
    print("Verification key is", verkey)
    return pubkey, verkey


def getStewardKeyFromName(baseDir, name):
    return getLocalVerKey(name, baseDir)


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

    return 'add genesis transaction NEW_NODE with data {"' + name + '": {' \
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
    return 'add genesis transaction NEW_NODE for ' + verkey + ' by ' + \
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
    return 'add genesis transaction NYM with data {"' + name + '": {' \
                                                                       '"verkey": "' + verkey + '"}} role=STEWARD'


def getOldAddNewGenStewardCommand(name, verkey):
    return 'add genesis transaction NYM for ' + verkey + ' with data ' \
                                                                 '{"alias": ' \
                                                                 '"' + name +\
           '"} role=STEWARD'


def generateStewardGenesisTxn(baseDir, displayTxn, name, verkey):
    txn = getOldAddNewGenStewardCommand(name, verkey)
    storeGenTxns(baseDir, txn)
    printGenTxn(txn, displayTxn)


def printGenTxn(txn, displayTxn):
    if displayTxn:
        print('\n' + txn)


def getVerKeyFromName(baseDir, roleName):
    return getLocalVerKey(roleName, baseDir)


def getPubKeyFromName(baseDir, roleName):
    return getLocalPubKey(roleName, baseDir)


def exportNodeGenTxn(baseDir, displayTxn, name):
    nodeInfo = getNodeInfo(baseDir, name)
    nodeVerKey = getVerKeyFromName(baseDir, name)
    stewardKey = nodeInfo.get('steward')
    nodeAddr = nodeInfo.get('nodeAddr')
    clientAddr = nodeInfo.get('clientAddr')

    txn = 'add genesis transaction NEW_NODE with data {"' + name + '": {' \
                                                                   '"verkey":' \
                                                                   ' "' + \
          nodeVerKey + \
          '", "node_address": "' + nodeAddr + \
          '", "client_address": "' + clientAddr + '"}, "by":"' + stewardKey +\
          '"}'
    storeExportedTxns(baseDir, txn)
    printGenTxn(txn, displayTxn)


def exportStewardGenTxn(baseDir, displayTxn, name):
    verkey = getLocalVerKey(name, baseDir)
    txn = 'add genesis transaction NYM with data  {"' + name + '": {' \
                                                                       '"verkey": "' + verkey + '"}} role=STEWARD'
    storeExportedTxns(baseDir, txn)
    printGenTxn(txn, displayTxn)


def submitNodeIpChange(client, stewardWallet, name: str, nym: str,
                       nodeStackHa: HA, clientStackHa: HA):
    (nodeIp, nodePort), (clientIp, clientPort) = nodeStackHa, clientStackHa
    txn = {
        TXN_TYPE: CHANGE_HA,
        TARGET_NYM: nym,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: int(nodePort),
            CLIENT_IP: clientIp,
            CLIENT_PORT: int(clientPort),
            ALIAS: name
        }
    }
    signedOp = stewardWallet.signOp(txn, stewardWallet.defaultId)
    req, = client.submitReqs(signedOp)
    return req


def changeHA(basedirpath):
    parser = argparse.ArgumentParser(
        description="Change Ip and/or Port of existing node")

    parser.add_argument('--env', required=True, type=str,
                        help='Environment, options are: test, live')

    parser.add_argument('--name', required=True, type=str,
                        help='node name')

    parser.add_argument('--nodeverkey', required=True, type=str,
                        help="node's ver key")

    parser.add_argument('--stewardseed', required=True, type=str,
                        help="stewards's seed")

    parser.add_argument('--nodestacknewha', required=True, type=str,
                        help="node stack's new HA, format=> ip:port")

    parser.add_argument('--clientstacknewha', required=False, type=str,
                        help="client stack's new HA, format=> ip:port")

    args = parser.parse_args()

    env = args.env
    nodeName = args.name
    nodeStackNewHA = args.nodestacknewha
    clientStackNewHA = args.clientstacknewha
    nodesNewIp, nodesNewPort = tuple(nodeStackNewHA.split(':'))
    nodeStackNewHA = (nodesNewIp, nodesNewPort)
    if not clientStackNewHA:
        clientStackNewHA = (nodesNewIp, str(int(nodesNewPort)+1))

    nodeVerKey = args.nodeverkey

    stewardsSeed = bytes(args.stewardseed, 'utf-8')
    stewardSigner = SimpleSigner(seed=stewardsSeed)

    stewardWallet = Wallet("StewardWallet")
    stewardWallet.addIdentifier(signer=stewardSigner)

    client_address = ('0.0.0.0', 9761)

    config=getConfig()
    config.poolTransactionsFile = ENVS[env].poolLedger
    config.domainTransactionsFile = ENVS[env].domainLedger
    client = Client("changehasteward",
                    ha=client_address,
                    config=config,
                    basedirpath=basedirpath)

    f = getMaxFailures(len(client.nodeReg))

    looper = Looper(debug=True)
    looper.add(client)
    looper.runFor(3)
    req = submitNodeIpChange(client, stewardWallet, nodeName, nodeVerKey,
                       nodeStackNewHA, clientStackNewHA)

    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId
                          , f, retryWait=1, timeout=8))
    looper.runFor(10)