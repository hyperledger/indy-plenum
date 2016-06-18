import base64

from raet.nacling import Privateer

from plenum.client.signer import SimpleSigner
from plenum.common.raet import initLocalKeep
from plenum.common.txn import TXN_TYPE, ORIGIN, TARGET_NYM, DATA, PUBKEY, ALIAS, \
    NEW_NODE, NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, CHANGE_HA, NEW_STEWARD, \
    CHANGE_KEYS, VERKEY
from plenum.common.util import randomString
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd, genHa, TestNode, \
    TestClient


def addNewClient(typ, looper, client, name):
    newSigner = SimpleSigner()
    pkseed = randomString(32).encode()
    priver = Privateer(pkseed)
    req, = client.submit({
        TXN_TYPE: typ,
        ORIGIN: client.defaultIdentifier,
        TARGET_NYM: newSigner.verstr,
        DATA: {
            PUBKEY: priver.pubhex.decode(),
            ALIAS: name
        }
    })
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    return newSigner


def addNewNode(looper, client, newNodeName, tdir, tconf):
    sigseed = randomString(32).encode()
    pkseed = randomString(32).encode()
    newSigner = SimpleSigner(seed=sigseed)
    priver = Privateer(pkseed)
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)
    req, = client.submit({
        TXN_TYPE: NEW_NODE,
        ORIGIN: client.defaultIdentifier,
        TARGET_NYM: newSigner.verstr,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: nodePort,
            CLIENT_IP: clientIp,
            CLIENT_PORT: clientPort,
            PUBKEY: priver.pubhex.decode(),
            ALIAS: newNodeName
        }
    })
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    initLocalKeep(newNodeName, tdir, pkseed, sigseed,
                  override=True)
    node = TestNode(newNodeName, basedirpath=tdir, config=tconf,
                    ha=(nodeIp, nodePort), cliha=(clientIp, clientPort))
    looper.add(node)
    return node


def addNewStewardAndNode(looper, client, stewardName, newNodeName, nodeReg,
                         tdir, tconf):
    newStewardSigner = addNewClient(NEW_STEWARD, looper, client, stewardName)
    newSteward = TestClient(name=stewardName,
                            nodeReg=nodeReg, ha=genHa(),
                            signer=newStewardSigner,
                            basedirpath=tdir)

    looper.add(newSteward)
    looper.run(newSteward.ensureConnectedToNodes())
    newNode = addNewNode(looper, newSteward, newNodeName, tdir, tconf)
    return newSteward, newNode


def changeNodeIp(looper, client, node, nodeHa, clientHa, baseDir, conf):
    nodeNym = base64.b64encode(node.nodestack.local.signer.verraw).decode()
    (nodeIp, nodePort), (clientIp, clientPort) = nodeHa, clientHa
    req, = client.submit({
        TXN_TYPE: CHANGE_HA,
        ORIGIN: client.defaultIdentifier,
        TARGET_NYM: nodeNym,
        DATA: {
            NODE_IP: nodeIp,
            NODE_PORT: nodePort,
            CLIENT_IP: clientIp,
            CLIENT_PORT: clientPort,
            ALIAS: node.name
        }
    })
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    node.nodestack.clearLocalKeep()
    node.nodestack.clearRemoteKeeps()
    node.clientstack.clearLocalKeep()
    node.clientstack.clearRemoteKeeps()


def changeNodeKeys(looper, client, node, verkey, pubkey, baseDir, conf):
    nodeNym = base64.b64encode(node.nodestack.local.signer.verraw).decode()
    req, = client.submit({
        TXN_TYPE: CHANGE_KEYS,
        ORIGIN: client.defaultIdentifier,
        TARGET_NYM: nodeNym,
        DATA: {
            PUBKEY: pubkey,
            VERKEY: verkey,
            ALIAS: node.name
        }
    })
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    node.nodestack.clearLocalRoleKeep()
    node.nodestack.clearRemoteRoleKeeps()
    node.nodestack.clearAllDir()
    node.clientstack.clearLocalRoleKeep()
    node.clientstack.clearRemoteRoleKeeps()
    node.clientstack.clearAllDir()
