import pytest

from plenum.test.cli.helper import checkRequest


@pytest.mark.skipif('sys.platform == "win32"', reason='SOV-457')
def testClientRequestWithPluginLoaded(cli,
                                      validNodeNames,
                                      loadOpVerificationPlugin,
                                      createAllNodes):
    # verify plugin is loaded
    for nodeName, node in cli.nodes.items():
        assert node.opVerifiers is not None
        assert len(node.opVerifiers) == 1
    operation = '{"name": "John", "age": "10", "type": "random"}'
    checkRequest(cli, operation)
