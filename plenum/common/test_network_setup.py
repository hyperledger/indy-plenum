import argparse
import ipaddress
import os
from collections import namedtuple

from ledger.ledger import Ledger

from ledger.serializers.compact_serializer import CompactSerializer
from stp_core.crypto.nacl_wrappers import Signer

from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.member.member import Member
from plenum.common.member.steward import Steward

from plenum.common.keygen_utils import initLocalKeys
from plenum.common.constants import STEWARD, CLIENT_STACK_SUFFIX, TRUSTEE
from plenum.common.util import hexToFriendly
from plenum.common.signer_did import DidSigner
from stp_core.common.util import adict


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
    def writeNodeParamsFile(filePath, name, nPort, cPort):
        contents = [
            'NODE_NAME={}'.format(name),
            'NODE_PORT={}'.format(nPort),
            'NODE_CLIENT_PORT={}'.format(cPort)
        ]
        with open(filePath, 'w') as f:
            f.writelines(os.linesep.join(contents))

    @classmethod
    def bootstrapTestNodesCore(cls, config, envName, appendToLedgers,
                               domainTxnFieldOrder, trustee_def, steward_defs,
                               node_defs, client_defs, localNodes, nodeParamsFileName):

        if not localNodes:
            localNodes = {}
        try:
            if isinstance(localNodes, int):
                _localNodes = {localNodes}
            else:
                _localNodes = {int(_) for _ in localNodes}
        except BaseException as exc:
            raise RuntimeError('nodeNum must be an int or set of ints') from exc

        baseDir = cls.setup_base_dir(config)

        poolLedger = cls.init_pool_ledger(appendToLedgers, baseDir, config,
                                          envName)

        domainLedger = cls.init_domain_ledger(appendToLedgers, baseDir, config,
                                              envName, domainTxnFieldOrder)

        trustee_txn = Member.nym_txn(trustee_def.nym, trustee_def.name, verkey=trustee_def.verkey, role=TRUSTEE)
        domainLedger.add(trustee_txn)

        for sd in steward_defs:
            nym_txn = Member.nym_txn(sd.nym, sd.name, verkey=sd.verkey, role=STEWARD,
                                     creator=trustee_def.nym)
            domainLedger.add(nym_txn)

        for nd in node_defs:

            if nd.idx in _localNodes:
                _, verkey = initLocalKeys(nd.name, baseDir,
                                          nd.sigseed, True, config=config)
                _, verkey = initLocalKeys(nd.name+CLIENT_STACK_SUFFIX, baseDir,
                                          nd.sigseed, True, config=config)
                verkey = verkey.encode()
                assert verkey == nd.verkey

                if nd.ip != '127.0.0.1':
                    paramsFilePath = os.path.join(baseDir, nodeParamsFileName)
                    print('Nodes will not run locally, so writing '
                          '{}'.format(paramsFilePath))
                    TestNetworkSetup.writeNodeParamsFile(
                        paramsFilePath, nd.name, nd.port, nd.client_port)

                print("This node with name {} will use ports {} and {} for "
                      "nodestack and clientstack respectively"
                      .format(nd.name, nd.port, nd.client_port))
            else:
                verkey = nd.verkey
            node_nym = cls.getNymFromVerkey(verkey)

            node_txn = Steward.node_txn(nd.steward_nym, nd.name, node_nym,
                                        nd.ip, nd.port, nd.client_port)
            poolLedger.add(node_txn)

        for cd in client_defs:
            txn = Member.nym_txn(cd.nym, cd.name, verkey=cd.verkey, creator=trustee_def.nym)
            domainLedger.add(txn)

        poolLedger.stop()
        domainLedger.stop()

    @classmethod
    def init_pool_ledger(cls, appendToLedgers, baseDir, config, envName):
        poolTxnFile = cls.pool_ledger_file_name(config, envName)
        pool_ledger = Ledger(CompactMerkleTree(), dataDir=baseDir,
                             fileName=poolTxnFile)
        if not appendToLedgers:
            pool_ledger.reset()
        return pool_ledger

    @classmethod
    def init_domain_ledger(cls, appendToLedgers, baseDir, config, envName,
                           domainTxnFieldOrder):
        domainTxnFile = cls.domain_ledger_file_name(config, envName)
        ser = CompactSerializer(fields=domainTxnFieldOrder)
        domain_ledger = Ledger(CompactMerkleTree(), serializer=ser,
                               dataDir=baseDir, fileName=domainTxnFile)
        if not appendToLedgers:
            domain_ledger.reset()
        return domain_ledger

    @classmethod
    def pool_ledger_file_name(cls, config, envName):
        if hasattr(config, "ENVS") and envName:
            return config.ENVS[envName].poolLedger
        else:
            return config.poolTransactionsFile

    @classmethod
    def domain_ledger_file_name(cls, config, envName):
        if hasattr(config, "ENVS") and envName:
            return config.ENVS[envName].domainLedger
        else:
            return config.domainTransactionsFile

    @classmethod
    def setup_base_dir(cls, config):
        baseDir = config.baseDir
        if not os.path.exists(baseDir):
            os.makedirs(baseDir, exist_ok=True)
        return baseDir

    @classmethod
    def bootstrapTestNodes(cls, config, startingPort, nodeParamsFileName, domainTxnFieldOrder):

        parser = argparse.ArgumentParser(
            description="Generate pool transactions for testing")

        parser.add_argument('--nodes', required=True,
                            help='node count should be less than 100',
                            type=cls._bootstrapArgsTypeNodeCount,
                            )
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
                            type=cls._bootstrapArgsTypeIps)

        parser.add_argument('--envName',
                            help='Environment name (test or live)',
                            type=str,
                            choices=('test', 'live'),
                            default="test",
                            required=False)

        parser.add_argument('--appendToLedgers',
                            help="Determine if ledger files needs to be erased "
                                 "before writing new information or not.",
                            action='store_true')

        args = parser.parse_args()

        if args.nodeNum:
            assert 0 <= args.nodeNum <= args.nodes, \
                "nodeNum should be less ore equal to nodeCount"

        steward_defs, node_defs = cls.gen_defs(args.ips, args.nodes, startingPort)
        client_defs = cls.gen_client_defs(args.clients)
        trustee_def = cls.gen_trustee_def(1)
        cls.bootstrapTestNodesCore(config, args.envName, args.appendToLedgers,
                                   domainTxnFieldOrder, trustee_def,
                                   steward_defs, node_defs, client_defs,
                                   args.nodeNum, nodeParamsFileName)

    @staticmethod
    def _bootstrapArgsTypeNodeCount(nodesStrArg):
        if not nodesStrArg.isdigit():
            raise argparse.ArgumentTypeError('should be a number')
        n = int(nodesStrArg)
        if n > 100:
            raise argparse.ArgumentTypeError(
                "Cannot run {} nodes for testing purposes as of now. "
                "This is not a problem with the protocol but some placeholder "
                "rules we put in place which will be replaced by our "
                "Governance model. Going to run only 100".format(n)
            )
        if n <= 0:
            raise argparse.ArgumentTypeError('should be > 0')
        return n

    @staticmethod
    def _bootstrapArgsTypeIps(ipsStrArg):
        ips = []
        for ip in ipsStrArg.split(','):
            ip = ip.strip()
            try:
                ipaddress.ip_address(ip)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    "'{}' is an invalid IP address".format(ip)
                )
            else:
                ips.append(ip)
        return ips

    @classmethod
    def gen_defs(cls, ips, nodeCount, starting_port):
        """
        Generates some default steward and node definitions for tests
        :param ips: array of ip addresses
        :param nodeCount: number of stewards/nodes
        :param starting_port: ports are assigned incremental starting with this
        :return: duple of steward and node definitions
        """
        if not ips:
            ips = ['127.0.0.1'] * nodeCount
        else:
            if len(ips) != nodeCount:
                if len(ips) > nodeCount:
                    ips = ips[:nodeCount]
                else:
                    ips += ['127.0.0.1'] * (nodeCount - len(ips))

        steward_defs = []
        node_defs = []
        for i in range(1, nodeCount + 1):
            d = adict()
            d.name = "Steward" + str(i)
            d.sigseed = cls.getSigningSeed(d.name)
            s_signer = DidSigner(seed=d.sigseed)
            d.nym = s_signer.identifier
            d.verkey = s_signer.verkey
            steward_defs.append(d)

            name = "Node" + str(i)
            sigseed = cls.getSigningSeed(name)
            node_defs.append(NodeDef(
                name=name,
                ip=ips[i - 1],
                port=starting_port + (i * 2) - 1,
                client_port=starting_port + (i * 2),
                idx=i,
                sigseed=sigseed,
                verkey=Signer(sigseed).verhex,
                steward_nym=d.nym))
        return steward_defs, node_defs

    @classmethod
    def gen_client_def(cls, idx):
        d = adict()
        d.name = "Client" + str(idx)
        d.sigseed = cls.getSigningSeed(d.name)
        c_signer = DidSigner(seed=d.sigseed)
        d.nym = c_signer.identifier
        d.verkey = c_signer.verkey
        return d

    @classmethod
    def gen_client_defs(cls, clientCount):
        return [cls.gen_client_def(idx) for idx in range(1, clientCount + 1)]

    @classmethod
    def gen_trustee_def(cls, idx):
        d = adict()
        d.name = 'Trustee' + str(idx)
        d.sigseed = cls.getSigningSeed(d.name)
        t_signer = DidSigner(seed=d.sigseed)
        d.nym = t_signer.identifier
        d.verkey = t_signer.verkey
        return d


NodeDef = namedtuple('NodeDef', 'name, ip, port, client_port, '
                                'idx, sigseed, verkey, steward_nym')
