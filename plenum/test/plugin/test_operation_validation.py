import os

import pytest

from plenum.server.node import Node
from plenum.test.helper import TestNodeSet


@pytest.yield_fixture(scope="module")
def nodeSet(tdir, nodeReg):
    """
    Overrides the fixture from conftest.py
    """
    curPath = os.path.dirname(os.path.abspath(__file__))
    pluginPath = os.path.join(curPath, 'operation_verification')
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir,
                     opVerificationPluginPath=pluginPath) as ns:

        for n in ns:  # type: Node
            assert n.opVerifiers is not None
            assert len(n.opVerifiers) == 1
            opVerifier, = n.opVerifiers
            assert opVerifier.count == 0

        yield ns


def testWithOpValidationPlugin(nodeSet, replied1):
    for n in nodeSet:  # type: Node
        opVerifier, = n.opVerifiers
        assert opVerifier.count == 1
