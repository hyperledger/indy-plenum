import argparse
import os
from _sha256 import sha256
from base64 import b64encode
from binascii import unhexlify

from raet.nacling import Privateer, Signer

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger

from plenum.common.raet import initLocalKeep
from plenum.common.txn import TARGET_NYM, TXN_TYPE, NEW_STEWARD, DATA, ALIAS, \
    PUBKEY, TXN_ID, NEW_NODE, CLIENT_IP, CLIENT_PORT, NODE_IP, NODE_PORT, \
    NEW_CLIENT
from plenum.common.types import f


class TestNetworkSetup:
    @staticmethod
    def getNumberFromName(name: str) -> int:
        if name.startswith("Node"):
            return int(name[4:])
        elif name.startswith("Steward"):
            return int(name[7:])
        elif name.startswith("Client"):
            return int(name[6:])
        else:
            raise ValueError("Cannot get number from {}".format(name))

    @staticmethod
    def getSigningSeed(name: str) -> bytes:
        return ('0' * (32 - len(name)) + name).encode()

    @staticmethod
    def getPKSeed(name: str) -> bytes:
        return (name + '0' * (32 - len(name))).encode()

    @staticmethod
    def getNymFromVerkey(verkey: bytes):
        return b64encode(unhexlify(verkey)).decode()

    @staticmethod
    def bootstrapTestNodes(startingPort, baseDir, poolTransactionsFile):
        if not os.path.exists(baseDir):
            os.makedirs(baseDir, exist_ok=True)

        parser = argparse.ArgumentParser(
            description="Generate pool transactions for testing")

        parser.add_argument('--nodes', required=True, type=int, help='node count, '
                                                                     'should be less than 20')
        parser.add_argument('--clients', required=True, type=int,
                            help='client count')
        parser.add_argument('--nodeNum', required=True, type=int, help='the number '
                                                                       'of the node that will run on this machine')
        parser.add_argument('--ips',
                            help='IPs of the nodes, provide comma separated'
                                 ' IPs, if no of IPs provided are less than number of nodes then the '
                                 'remaining nodes are assigned the loopback IP, i.e 127.0.0.1',
                            type=str)

        args = parser.parse_args()
        nodeCount = min(args.nodes, 20)
        clientCount = args.clients
        nodeNum = args.nodeNum
        ips = args.ips

        assert nodeNum <= nodeCount, "nodeNum should be less than equal to nodeCount"

        if not ips:
            ips = ['127.0.0.1'] * nodeCount
        else:
            ips = ips.split(",")
            if len(ips) != nodeCount:
                if len(ips) > nodeCount:
                    ips = ips[:nodeCount]
                else:
                    ips = ips + ['127.0.0.1'] * (nodeCount - len(ips))

        ledger = Ledger(CompactMerkleTree(),
                        dataDir=baseDir,
                        fileName=poolTransactionsFile)
        ledger.reset()

        steward1Nym = None
        for num in range(1, nodeCount + 1):
            stewardName = "Steward" + str(num)
            pkseed, sigseed = TestNetworkSetup.getPKSeed(stewardName), \
                              TestNetworkSetup.getSigningSeed(stewardName)
            pubkey, verkey = Privateer(pkseed).pubhex, Signer(sigseed).verhex
            stewardNym = TestNetworkSetup.getNymFromVerkey(verkey)
            txn = {
                TARGET_NYM: stewardNym,
                TXN_TYPE: NEW_STEWARD,
                DATA: {
                    ALIAS: stewardName,
                    PUBKEY: pubkey.decode()
                },
                TXN_ID: sha256(stewardName.encode()).hexdigest()
            }
            if num == 1:
                steward1Nym = stewardNym
            else:
                # The first steward adds every steward
                txn[f.IDENTIFIER.nm] = steward1Nym
            ledger.add(txn)

            nodeName = "Node" + str(num)
            nodePort, clientPort = startingPort + (num * 2 - 1), startingPort + (
            num * 2)
            ip = ips[num - 1]
            pkseed, sigseed = TestNetworkSetup.getPKSeed(nodeName), \
                              TestNetworkSetup.getSigningSeed(nodeName)
            if nodeNum == num:
                pubkey, verkey = initLocalKeep(nodeName, baseDir, pkseed, sigseed,
                                               True)
                pubkey, verkey = pubkey.encode(), verkey.encode()
            else:
                pubkey, verkey = Privateer(pkseed).pubhex, Signer(sigseed).verhex
            txn = {
                TARGET_NYM: TestNetworkSetup.getNymFromVerkey(verkey),
                TXN_TYPE: NEW_NODE,
                f.IDENTIFIER.nm: stewardNym,
                DATA: {
                    CLIENT_IP: ip,
                    PUBKEY: pubkey.decode(),
                    ALIAS: nodeName,
                    CLIENT_PORT: clientPort,
                    NODE_IP: ip,
                    NODE_PORT: nodePort
                },
                TXN_ID: sha256(nodeName.encode()).hexdigest()
            }
            ledger.add(txn)

        for num in range(1, clientCount + 1):
            clientName = "Client" + str(num)
            pkseed, sigseed = TestNetworkSetup.getPKSeed(clientName), \
                              TestNetworkSetup.getSigningSeed(clientName)
            pubkey, verkey = Privateer(pkseed).pubhex, Signer(sigseed).verhex
            txn = {
                f.IDENTIFIER.nm: steward1Nym,
                TARGET_NYM: TestNetworkSetup.getNymFromVerkey(verkey),
                TXN_TYPE: NEW_CLIENT,
                DATA: {
                    ALIAS: clientName,
                    PUBKEY: pubkey.decode()
                },
                TXN_ID: sha256(clientName.encode()).hexdigest()
            }
            ledger.add(txn)

        ledger.stop()