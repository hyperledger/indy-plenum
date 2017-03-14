import argparse
import os
from hashlib import sha256

from ledger.serializers.compact_serializer import CompactSerializer
from raet.nacling import Signer

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.ledger import Ledger

from plenum.common.raet import initLocalKeep
from plenum.common.txn import TARGET_NYM, TXN_TYPE, DATA, ALIAS, \
    TXN_ID, NODE, CLIENT_IP, CLIENT_PORT, NODE_IP, NODE_PORT, NYM, \
    STEWARD, \
    ROLE, SERVICES, VALIDATOR, TRUSTEE
from plenum.common.types import f
from plenum.common.util import hexToFriendly


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
    def getNymFromVerkey(verkey: bytes):
        return hexToFriendly(verkey)

    @staticmethod
    def bootstrapTestNodesCore(config, envName, appendToLedgers,
                               domainTxnFieldOrder,
                               ips, nodeCount, clientCount,
                               nodeNum, startingPort):

        baseDir = config.baseDir
        if not os.path.exists(baseDir):
            os.makedirs(baseDir, exist_ok=True)

        if not ips:
            ips = ['127.0.0.1'] * nodeCount
        else:
            ips = ips.split(",")
            if len(ips) != nodeCount:
                if len(ips) > nodeCount:
                    ips = ips[:nodeCount]
                else:
                    ips += ['127.0.0.1'] * (nodeCount - len(ips))

        if hasattr(config, "ENVS") and envName:
            poolTxnFile = config.ENVS[envName].poolLedger
            domainTxnFile = config.ENVS[envName].domainLedger
        else:
            poolTxnFile = config.poolTransactionsFile
            domainTxnFile = config.domainTransactionsFile

        poolLedger = Ledger(CompactMerkleTree(),
                            dataDir=baseDir,
                            fileName=poolTxnFile)

        domainLedger = Ledger(CompactMerkleTree(),
                              serializer=CompactSerializer(fields=
                                                           domainTxnFieldOrder),
                              dataDir=baseDir,
                              fileName=domainTxnFile)

        if not appendToLedgers:
            poolLedger.reset()
            domainLedger.reset()

        trusteeName = "Trustee1"
        sigseed = TestNetworkSetup.getSigningSeed(trusteeName)
        verkey = Signer(sigseed).verhex
        trusteeNym = TestNetworkSetup.getNymFromVerkey(verkey)
        txn = {
            TARGET_NYM: trusteeNym,
            TXN_TYPE: NYM,
            # TODO: Trustees dont exist in Plenum, but only in Sovrin.
            # This should be moved to Sovrin
            ROLE: TRUSTEE,
            ALIAS: trusteeName,
            TXN_ID: sha256(trusteeName.encode()).hexdigest()
        }
        domainLedger.add(txn)

        steward1Nym = None
        for num in range(1, nodeCount + 1):
            stewardName = "Steward" + str(num)
            sigseed = TestNetworkSetup.getSigningSeed(stewardName)
            verkey = Signer(sigseed).verhex
            stewardNym = TestNetworkSetup.getNymFromVerkey(verkey)
            txn = {
                TARGET_NYM: stewardNym,
                TXN_TYPE: NYM,
                ROLE: STEWARD,
                ALIAS: stewardName,
                TXN_ID: sha256(stewardName.encode()).hexdigest()
            }
            if num == 1:
                steward1Nym = stewardNym
            else:
                # The first steward adds every steward
                txn[f.IDENTIFIER.nm] = steward1Nym
            domainLedger.add(txn)

            nodeName = "Node" + str(num)
            nodePort, clientPort = startingPort + (num * 2 - 1), startingPort \
                                   + (num * 2)
            ip = ips[num - 1]
            sigseed = TestNetworkSetup.getSigningSeed(nodeName)
            if nodeNum == num:
                _, verkey = initLocalKeep(nodeName, baseDir, sigseed, True)
                verkey = verkey.encode()
                print("This node with name {} will use ports {} and {} for "
                      "nodestack and clientstack respectively"
                      .format(nodeName, nodePort, clientPort))
            else:
                verkey = Signer(sigseed).verhex
            txn = {
                TARGET_NYM: TestNetworkSetup.getNymFromVerkey(verkey),
                TXN_TYPE: NODE,
                f.IDENTIFIER.nm: stewardNym,
                DATA: {
                    CLIENT_IP: ip,
                    ALIAS: nodeName,
                    CLIENT_PORT: clientPort,
                    NODE_IP: ip,
                    NODE_PORT: nodePort,
                    SERVICES: [VALIDATOR]
                },
                TXN_ID: sha256(nodeName.encode()).hexdigest()
            }
            poolLedger.add(txn)

        for num in range(1, clientCount + 1):
            clientName = "Client" + str(num)
            sigseed = TestNetworkSetup.getSigningSeed(clientName)
            verkey = Signer(sigseed).verhex
            txn = {
                f.IDENTIFIER.nm: steward1Nym,
                TARGET_NYM: TestNetworkSetup.getNymFromVerkey(verkey),
                TXN_TYPE: NYM,
                ALIAS: clientName,
                TXN_ID: sha256(clientName.encode()).hexdigest()
            }
            domainLedger.add(txn)

        poolLedger.stop()
        domainLedger.stop()

    @staticmethod
    def bootstrapTestNodes(config, startingPort, domainTxnFieldOrder):

        parser = argparse.ArgumentParser(
            description="Generate pool transactions for testing")

        parser.add_argument('--nodes', required=True, type=int,
                            help='node count, '
                                 'should be less than 20')
        parser.add_argument('--clients', required=True, type=int,
                            help='client count')
        parser.add_argument('--nodeNum', type=int,
                            help='the number of the node that will '
                                 'run on this machine')
        parser.add_argument('--ips',
                            help='IPs of the nodes, provide comma separated'
                                 ' IPs, if no of IPs provided are less than '
                                 'number of nodes then the '
                                 'remaining nodes are assigned the loopback '
                                 'IP, i.e 127.0.0.1',
                            type=str)

        parser.add_argument('--envName',
                            help='Environment name (test or live)',
                            type=str,
                            default="test",
                            required=False)

        parser.add_argument('--appendToLedgers',
                            help="Determine if ledger files needs to be erased "
                                 "before writting new information or not.",
                            action='store_true')

        args = parser.parse_args()
        if args.nodes > 20:
            print("Cannot run {} nodes for testing purposes as of now. "
                  "This is not a problem with the protocol but some placeholder"
                  " rules we put in place which will be replaced by our "
                  "Governance model. Going to run only 20".format(args.nodes))
            nodeCount = 20
        else:
            nodeCount = args.nodes
        clientCount = args.clients
        nodeNum = args.nodeNum
        ips = args.ips
        envName = args.envName
        appendToLedgers = args.appendToLedgers
        if nodeNum:
            assert nodeNum <= nodeCount, "nodeNum should be less than equal " \
                                         "to nodeCount"

        TestNetworkSetup.bootstrapTestNodesCore(config, envName, appendToLedgers,
                                                domainTxnFieldOrder,
                                                ips, nodeCount, clientCount,
                                                nodeNum, startingPort)
