
import pytest

from plenum.server.node import Node
from plenum.test.test_node import TestNodeSet
from plenum.test.plugin.conftest import OPERATION_VALIDATION_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import getPluginPath


@pytest.fixture(scope="module")
def pluginVerPath():
    return getPluginPath(OPERATION_VALIDATION_PLUGIN_PATH_VALUE)


@pytest.fixture(scope="module")
def allPluginPaths(pluginVerPath):
    return [pluginVerPath]


@pytest.yield_fixture(scope="module")
def nodeSet(tdir, nodeReg, allPluginPaths):
    """
    Overrides the fixture from conftest.py
    """
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir,
                     pluginPaths=allPluginPaths) as ns:

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
