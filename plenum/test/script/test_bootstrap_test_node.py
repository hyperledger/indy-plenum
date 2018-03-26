from argparse import ArgumentTypeError

import pytest

from plenum.common.test_network_setup import TestNetworkSetup
from plenum.common.txn_util import getTxnOrderedFields
from plenum.common.util import randomString

portsStart = 9600


@pytest.fixture(scope='module')
def params(tconf):
    steward_defs, node_defs = TestNetworkSetup.gen_defs(
        ips=None, nodeCount=4, starting_port=portsStart)

    client_defs = TestNetworkSetup.gen_client_defs(clientCount=1)
    trustee_def = TestNetworkSetup.gen_trustee_def(1)
    nodeParamsFile = randomString()
    return steward_defs, node_defs, client_defs, trustee_def, nodeParamsFile


def testBootstrapTestNode(params, tdir, tconf):
    # TODO: Need to add some asserts

    steward_defs, node_defs, client_defs, trustee_def, nodeParamsFile = params
    TestNetworkSetup.bootstrapTestNodesCore(
        config=tconf, network="test", appendToLedgers=False,
        domainTxnFieldOrder=getTxnOrderedFields(),
        trustee_def=trustee_def, steward_defs=steward_defs,
        node_defs=node_defs, client_defs=client_defs, localNodes=1,
        nodeParamsFileName=nodeParamsFile, chroot=tdir)


def test_check_valid_ip_host(params, tdir, tconf):
    _, _, client_defs, trustee_def, nodeParamsFile = params

    valid = [
        '34.200.79.65,52.38.24.189',
        'ec2-54-173-9-185.compute-1.amazonaws.com,ec2-52-38-24-189.compute-1.amazonaws.com',
        'ec2-54-173-9-185.compute-1.amazonaws.com,52.38.24.189,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.compute-1.amazonaws.com,34.200.79.65',
        'ledger.net,ledger.net'
    ]

    invalid = [
        '34.200.79()3.65,52.38.24.189',
        '52.38.24.189,ec2-54-173$-9-185.compute-1.amazonaws.com,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.com$pute-1.amazonaws.com,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.com&pute-1.amazonaws.com,34.200.79.65',
        '52.38.24.189,ec2-54-173-9-185.com*pute-1.amazonaws.com,34.200.79.65',
    ]
    for v in valid:
        assert v.split(',') == TestNetworkSetup._bootstrap_args_type_ips_hosts(v)
        steward_defs, node_defs = TestNetworkSetup.gen_defs(
            ips=None, nodeCount=2, starting_port=portsStart)
        TestNetworkSetup.bootstrapTestNodesCore(
            config=tconf, network="test", appendToLedgers=False,
            domainTxnFieldOrder=getTxnOrderedFields(),
            trustee_def=trustee_def, steward_defs=steward_defs,
            node_defs=node_defs, client_defs=client_defs, localNodes=1,
            nodeParamsFileName=nodeParamsFile, chroot=tdir)

    for v in invalid:
        with pytest.raises(ArgumentTypeError):
            TestNetworkSetup._bootstrap_args_type_ips_hosts(v)
